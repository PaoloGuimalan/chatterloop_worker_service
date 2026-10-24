package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const fanoutQueue = "bulk_fanout_to_cache"

// BulkFanoutConsumer is also the queue adapter used at startup: processing and
// continuation errors reach the consumer's ACK decision unchanged.
func BulkFanoutConsumer() ConsumerConfig {
	return bulkFanoutConsumer(BulkFanoutToCache)
}

func bulkFanoutConsumer(run func(context.Context, BulkFanoutPayload) error) ConsumerConfig {
	return ConsumerConfig{
		Queue: fanoutQueue, Timeout: 10 * time.Second, RetryFailures: true,
		Handler: func(ctx context.Context, body []byte) error {
			var payload BulkFanoutPayload
			if err := json.Unmarshal(body, &payload); err != nil {
				return fmt.Errorf("%w: %v", ErrDrop, err)
			}
			if payload.PostData.ID == "" {
				return fmt.Errorf("%w: fanout post ID is missing", ErrDrop)
			}
			return run(ctx, payload)
		},
	}
}

func enqueueFanout(ctx context.Context, payload BulkFanoutPayload) error {
	if ActiveRabbitMQ == nil {
		return ErrNotConnected
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return ActiveRabbitMQ.publishFanoutContinuation(ctx, body)
}

// Continuations must be broker-confirmed before the current delivery is ACKed.
// An isolated short-lived channel keeps confirms and mandatory returns specific
// to this job, including after a reconnect or an uncertain previous publish.
// Ordinary queue publishers retain their existing channel and behavior.
func (rc *RabbitMQ) publishFanoutContinuation(ctx context.Context, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rc.mu.RLock()
	conn := rc.conn
	rc.mu.RUnlock()
	if conn == nil || conn.IsClosed() {
		return ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(fanoutQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.Confirm(false); err != nil {
		return err
	}
	returned := ch.NotifyReturn(make(chan amqp.Return, 1))
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", fanoutQueue, true, false, amqp.Publishing{
		ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: body,
	})
	if err != nil {
		return err
	}
	if confirmation == nil {
		return fmt.Errorf("fanout publisher confirms are unavailable")
	}
	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		return fmt.Errorf("broker rejected fanout continuation")
	}
	// RabbitMQ sends basic.return before its publisher confirmation. Both are
	// dispatched by the same channel reader, so a return is already buffered.
	select {
	case result := <-returned:
		return fmt.Errorf("fanout continuation was unroutable: %s", result.ReplyText)
	default:
		return nil
	}
}
