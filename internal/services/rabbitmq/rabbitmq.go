package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// heartbeat drives dead-connection detection. amqp091-go sends at half this
	// interval and tears the connection down after two missed beats, so a broker
	// or network death surfaces within ~20s instead of hanging forever.
	heartbeat = 10 * time.Second

	// Reconnect backoff bounds. CloudAMQP shared nodes bounce during maintenance,
	// so we retry indefinitely rather than giving up.
	reconnectMinDelay = 1 * time.Second
	reconnectMaxDelay = 30 * time.Second

	defaultPrefetch = 16
	defaultWorkers  = 4
	defaultTimeout  = 30 * time.Second

	// Grace period for in-flight handlers to finish on shutdown. Kept under
	// Docker's default 10s stop timeout so the drain completes before SIGKILL;
	// raise both together if handlers need longer.
	shutdownDrainTimeout = 8 * time.Second
)

// ErrDrop marks a message as permanently unprocessable (malformed JSON, unknown
// schema). Returning it acks the delivery instead of requeueing, so a poison
// message cannot spin forever.
var ErrDrop = errors.New("message is permanently unprocessable")

// ErrNotConnected is returned by Publish while the supervisor is reconnecting.
var ErrNotConnected = errors.New("rabbitmq is not connected")

// HandlerFunc processes one delivery. It runs synchronously: the delivery is
// acked only after it returns nil, so a crash or redeploy mid-handler leaves the
// message on the queue instead of losing it. ctx carries the per-queue timeout.
type HandlerFunc func(ctx context.Context, body []byte) error

// ConsumerConfig describes one queue subscription. Prefetch bounds how many
// deliveries the broker will hand over unacked, and Workers bounds how many run
// concurrently -- together they cap the load a backlog can put on downstream
// databases when the service restarts.
type ConsumerConfig struct {
	Queue    string
	Handler  HandlerFunc
	Prefetch int
	Workers  int
	Timeout  time.Duration
}

func (c *ConsumerConfig) applyDefaults() {
	if c.Prefetch <= 0 {
		c.Prefetch = defaultPrefetch
	}
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.Workers > c.Prefetch {
		// More workers than prefetched messages just leaves goroutines idle.
		c.Workers = c.Prefetch
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
}

// consumerChannel is a live subscription, tracked so shutdown can cancel it and
// let in-flight handlers ack before the connection goes away.
type consumerChannel struct {
	ch  *amqp.Channel
	tag string
}

// RabbitMQ owns a single connection plus one channel per consumer. Consumers get
// dedicated channels so that a channel-level exception on one queue cannot
// cancel the subscriptions of the other queues sharing the connection.
type RabbitMQ struct {
	connStr string
	host    string

	mu     sync.RWMutex
	conn   *amqp.Connection
	pubCh  *amqp.Channel
	active []consumerChannel

	// pubMu serialises publisher-side RPCs. amqp091-go does not allow concurrent
	// RPCs (QueueDeclare) on one channel.
	pubMu    sync.Mutex
	declared map[string]struct{}

	specs []ConsumerConfig

	// attachMu makes "check shutdown, then start workers" atomic with respect to
	// Close, so wg.Add can never race the wg.Wait in Close.
	attachMu sync.Mutex

	wg       sync.WaitGroup // in-flight consumer workers for the current connection
	shutdown chan struct{}
	closeOne sync.Once
}

// ActiveRabbitMQ is the process-wide client, set by RabbitClient.
var ActiveRabbitMQ *RabbitMQ

// RabbitClient validates configuration and opens the initial connection so a bad
// host or credential fails loudly at boot rather than silently at runtime.
func RabbitClient() (*RabbitMQ, error) {
	protocol := os.Getenv("RABBITMQ_PROTOCOL")
	user := os.Getenv("RABBITMQ_USER")
	pass := os.Getenv("RABBITMQ_PASS")
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	vhost := os.Getenv("RABBITMQ_VHOST")

	if protocol == "" || user == "" || pass == "" || host == "" || port == "" {
		return nil, fmt.Errorf("missing required RabbitMQ environment variables")
	}

	rc := &RabbitMQ{
		connStr:  fmt.Sprintf("%s://%s:%s@%s:%s/%s", protocol, user, pass, host, port, vhost),
		host:     host,
		declared: make(map[string]struct{}),
		shutdown: make(chan struct{}),
	}

	if err := rc.dial(); err != nil {
		return nil, err
	}

	ActiveRabbitMQ = rc

	return rc, nil
}

// dial opens the connection and the shared publisher channel.
func (rc *RabbitMQ) dial() error {
	slog.Info("Connecting to RabbitMQ", "host", rc.host)

	conn, err := amqp.DialConfig(rc.connStr, amqp.Config{
		Heartbeat: heartbeat,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("failed to dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open publisher channel: %w", err)
	}

	rc.mu.Lock()
	rc.conn = conn
	rc.pubCh = ch
	rc.active = nil // subscriptions from the previous connection are gone
	rc.mu.Unlock()

	rc.pubMu.Lock()
	rc.declared = make(map[string]struct{}) // queue cache is per-channel
	rc.pubMu.Unlock()

	slog.Info("Connected to RabbitMQ", "host", rc.host)

	return nil
}

// Register queues a consumer for Start. It performs no I/O, so all consumers can
// be declared up front and attached together.
func (rc *RabbitMQ) Register(cfg ConsumerConfig) {
	cfg.applyDefaults()
	rc.specs = append(rc.specs, cfg)
}

// Start attaches every registered consumer and launches the supervisor that
// re-attaches them after a connection loss. An attach failure here is fatal to
// startup by design: booting with no consumers is the failure mode that let
// queues silently pile up.
func (rc *RabbitMQ) Start() error {
	if len(rc.specs) == 0 {
		return fmt.Errorf("no consumers registered")
	}

	if err := rc.attachAll(); err != nil {
		return err
	}

	go rc.supervise()

	return nil
}

// attachAll subscribes every registered consumer on the current connection.
func (rc *RabbitMQ) attachAll() error {
	rc.attachMu.Lock()
	defer rc.attachMu.Unlock()

	if rc.isShuttingDown() {
		return ErrNotConnected
	}

	rc.mu.RLock()
	conn := rc.conn
	rc.mu.RUnlock()

	if conn == nil || conn.IsClosed() {
		return ErrNotConnected
	}

	for _, spec := range rc.specs {
		if err := rc.attach(conn, spec); err != nil {
			return fmt.Errorf("failed to attach consumer %q: %w", spec.Queue, err)
		}
	}

	slog.Info("RabbitMQ consumers attached", "count", len(rc.specs))

	return nil
}

// attach opens a dedicated channel for one queue and starts its worker pool.
func (rc *RabbitMQ) attach(conn *amqp.Connection, spec ConsumerConfig) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}

	if _, err := ch.QueueDeclare(spec.Queue, true, false, false, false, nil); err != nil {
		ch.Close()
		return fmt.Errorf("declare queue: %w", err)
	}

	// Without Qos the broker dumps the entire backlog into client memory at once.
	if err := ch.Qos(spec.Prefetch, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("set qos: %w", err)
	}

	// An explicit tag lets Close cancel this subscription by name.
	tag := fmt.Sprintf("worker-%s-%d", spec.Queue, time.Now().UnixNano())

	msgs, err := ch.Consume(
		spec.Queue,
		tag,
		false, // autoAck off: we ack after the handler succeeds
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("consume: %w", err)
	}

	rc.mu.Lock()
	rc.active = append(rc.active, consumerChannel{ch: ch, tag: tag})
	rc.mu.Unlock()

	// Workers compete over the same delivery channel; the range loop ends when
	// the broker or a connection loss closes it, which is how workers retire.
	var workers sync.WaitGroup
	for i := 0; i < spec.Workers; i++ {
		workers.Add(1)
		rc.wg.Add(1)
		go func() {
			defer workers.Done()
			defer rc.wg.Done()
			for d := range msgs {
				rc.process(spec, d)
			}
		}()
	}

	go func() {
		workers.Wait()
		ch.Close()

		if rc.isShuttingDown() {
			slog.Info("RabbitMQ consumer stopped", "queue", spec.Queue)
			return
		}

		// Unexpected: the supervisor should be reconnecting right now.
		slog.Warn("RabbitMQ consumer detached", "queue", spec.Queue)
	}()

	return nil
}

// process runs one handler and decides the delivery's fate. A panic is contained
// here so one bad message cannot take down the worker pool.
func (rc *RabbitMQ) process(spec ConsumerConfig, d amqp.Delivery) {
	ctx, cancel := context.WithTimeout(context.Background(), spec.Timeout)
	defer cancel()

	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.Error("handler panicked",
					"queue", spec.Queue,
					"panic", r,
					"stack", string(debug.Stack()),
				)
				err = fmt.Errorf("handler panicked: %v", r)
			}
		}()
		return spec.Handler(ctx, d.Body)
	}()

	switch {
	case err == nil:
		rc.ack(spec.Queue, d)

	case errors.Is(err, ErrDrop):
		slog.Error("dropping unprocessable message", "queue", spec.Queue, "err", err)
		rc.ack(spec.Queue, d)

	case d.Redelivered:
		// Already retried once. Requeueing again would loop forever, so drop it
		// with a loud log rather than block the queue.
		slog.Error("dropping message after failed retry", "queue", spec.Queue, "err", err)
		rc.ack(spec.Queue, d)

	default:
		slog.Error("requeueing failed message", "queue", spec.Queue, "err", err)
		if nackErr := d.Nack(false, true); nackErr != nil {
			slog.Error("nack failed", "queue", spec.Queue, "err", nackErr)
		}
	}
}

func (rc *RabbitMQ) ack(queue string, d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		// Expected when the connection dropped mid-handler: the broker will
		// redeliver, which is why handlers need to be idempotent.
		slog.Warn("ack failed, message will be redelivered", "queue", queue, "err", err)
	}
}

// supervise watches the connection and rebuilds every subscription after a loss.
// amqp091-go has no automatic recovery; without this, one dropped connection
// silently ends all consumption for the life of the process.
func (rc *RabbitMQ) supervise() {
	for {
		rc.mu.RLock()
		conn := rc.conn
		rc.mu.RUnlock()

		if conn == nil {
			if !rc.reconnect() {
				return
			}
			continue
		}

		closed := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-rc.shutdown:
			return

		case reason := <-closed:
			if rc.isShuttingDown() {
				return
			}

			slog.Error("RabbitMQ connection lost, reconnecting", "reason", reason)

			// Workers from the dead connection exit on their own once their
			// delivery channels close; wait so generations do not overlap.
			rc.wg.Wait()

			if !rc.reconnect() {
				return
			}
		}
	}
}

// reconnect retries dial + attach with exponential backoff. It returns false
// only when the client is shutting down.
func (rc *RabbitMQ) reconnect() bool {
	delay := reconnectMinDelay

	for attempt := 1; ; attempt++ {
		select {
		case <-rc.shutdown:
			return false
		case <-time.After(delay):
		}

		if rc.isShuttingDown() {
			return false
		}

		if err := rc.dial(); err != nil {
			slog.Error("RabbitMQ reconnect failed", "attempt", attempt, "err", err)
		} else if err := rc.attachAll(); err != nil {
			slog.Error("RabbitMQ consumer re-attach failed", "attempt", attempt, "err", err)
			rc.closeConn()
		} else {
			slog.Info("RabbitMQ reconnected", "attempt", attempt)
			return true
		}

		// Exponential backoff with jitter so parallel replicas do not stampede
		// the broker after a shared-node restart.
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
		delay += time.Duration(rand.Int63n(int64(time.Second)))
	}
}

func (rc *RabbitMQ) closeConn() {
	rc.mu.Lock()
	conn := rc.conn
	rc.conn = nil
	rc.pubCh = nil
	rc.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
}

func (rc *RabbitMQ) isShuttingDown() bool {
	select {
	case <-rc.shutdown:
		return true
	default:
		return false
	}
}

// Publish sends to a queue on the dedicated publisher channel, so a publish-side
// failure cannot cancel any consumer.
func (rc *RabbitMQ) Publish(ctx context.Context, queueName string, body []byte) error {
	rc.mu.RLock()
	ch := rc.pubCh
	rc.mu.RUnlock()

	if ch == nil || ch.IsClosed() {
		return ErrNotConnected
	}

	rc.pubMu.Lock()
	defer rc.pubMu.Unlock()

	if _, ok := rc.declared[queueName]; !ok {
		if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
			return fmt.Errorf("failed to declare queue on publish: %w", err)
		}
		rc.declared[queueName] = struct{}{}
	}

	return ch.PublishWithContext(ctx,
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survives a broker restart
			Body:         body,
		},
	)
}

// Healthy reports whether the client currently holds a live connection. Used by
// /status so a consumer outage is visible to orchestration instead of hiding
// behind a process that is still serving HTTP.
func (rc *RabbitMQ) Healthy() bool {
	if rc == nil {
		return false
	}

	rc.mu.RLock()
	defer rc.mu.RUnlock()

	return rc.conn != nil && !rc.conn.IsClosed()
}

// Consumers returns how many subscriptions are registered.
func (rc *RabbitMQ) Consumers() int {
	if rc == nil {
		return 0
	}
	return len(rc.specs)
}

// Close stops the supervisor and waits briefly for in-flight handlers so their
// messages get acked rather than redelivered.
func (rc *RabbitMQ) Close() {
	if rc == nil {
		return
	}

	rc.closeOne.Do(func() {
		close(rc.shutdown)
	})

	// Barrier: any attach already in flight finishes its wg.Add calls before we
	// tear down, and any attach that starts after this sees the shutdown flag.
	rc.attachMu.Lock()
	rc.attachMu.Unlock()

	rc.mu.RLock()
	active := append([]consumerChannel(nil), rc.active...)
	pub := rc.pubCh
	rc.mu.RUnlock()

	// Cancel before closing anything. This stops new deliveries while leaving the
	// connection up, so handlers already running can still ack. Tearing the
	// connection down first would fail those acks and re-run the work on reboot.
	for _, c := range active {
		if err := c.ch.Cancel(c.tag, false); err != nil {
			slog.Warn("failed to cancel consumer", "tag", c.tag, "err", err)
		}
	}

	done := make(chan struct{})
	go func() {
		rc.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("RabbitMQ handlers drained")
	case <-time.After(shutdownDrainTimeout):
		// Anything still running loses its ack and will be redelivered.
		slog.Warn("timed out waiting for RabbitMQ handlers to drain")
	}

	if pub != nil {
		pub.Close()
	}

	rc.closeConn()
}
