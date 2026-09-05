package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeAcknowledger records what the consumer decided to do with a delivery.
type fakeAcknowledger struct {
	acked        bool
	nacked       bool
	nackRequeued bool
	rejected     bool
}

func (f *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	f.acked = true
	return nil
}

func (f *fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	f.nacked = true
	f.nackRequeued = requeue
	return nil
}

func (f *fakeAcknowledger) Reject(tag uint64, requeue bool) error {
	f.rejected = true
	return nil
}

func TestProcessAckPolicy(t *testing.T) {
	errBoom := errors.New("downstream unavailable")

	tests := []struct {
		name         string
		handler      HandlerFunc
		redelivered  bool
		wantAck      bool
		wantNack     bool
		wantRequeued bool
	}{
		{
			name:    "success acks",
			handler: func(ctx context.Context, body []byte) error { return nil },
			wantAck: true,
		},
		{
			name:    "permanent failure is dropped, not requeued",
			handler: func(ctx context.Context, body []byte) error { return ErrDrop },
			wantAck: true,
		},
		{
			name: "wrapped ErrDrop is dropped",
			handler: func(ctx context.Context, body []byte) error {
				return fmt.Errorf("%w: bad json", ErrDrop)
			},
			wantAck: true,
		},
		{
			name:         "transient failure on first delivery is requeued",
			handler:      func(ctx context.Context, body []byte) error { return errBoom },
			wantNack:     true,
			wantRequeued: true,
		},
		{
			name:        "transient failure after a retry is dropped, not looped",
			handler:     func(ctx context.Context, body []byte) error { return errBoom },
			redelivered: true,
			wantAck:     true,
		},
		{
			name:         "panic is contained and requeued",
			handler:      func(ctx context.Context, body []byte) error { panic("boom") },
			wantNack:     true,
			wantRequeued: true,
		},
	}

	rc := &RabbitMQ{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ack := &fakeAcknowledger{}
			spec := ConsumerConfig{Queue: "test_queue", Handler: tt.handler}
			spec.applyDefaults()

			rc.process(spec, amqp.Delivery{
				Acknowledger: ack,
				DeliveryTag:  1,
				Redelivered:  tt.redelivered,
				Body:         []byte(`{}`),
			})

			if ack.acked != tt.wantAck {
				t.Errorf("acked = %v, want %v", ack.acked, tt.wantAck)
			}
			if ack.nacked != tt.wantNack {
				t.Errorf("nacked = %v, want %v", ack.nacked, tt.wantNack)
			}
			if ack.nacked && ack.nackRequeued != tt.wantRequeued {
				t.Errorf("nack requeue = %v, want %v", ack.nackRequeued, tt.wantRequeued)
			}
			if ack.rejected {
				t.Error("delivery was rejected; policy only uses ack and nack")
			}
		})
	}
}

// The handler must receive a context carrying the queue's timeout, so a hung
// downstream call cannot pin a worker forever.
func TestProcessAppliesTimeout(t *testing.T) {
	rc := &RabbitMQ{}
	ack := &fakeAcknowledger{}

	var deadline time.Time
	var ok bool

	spec := ConsumerConfig{
		Queue:   "test_queue",
		Timeout: 5 * time.Second,
		Handler: func(ctx context.Context, body []byte) error {
			deadline, ok = ctx.Deadline()
			return nil
		},
	}
	spec.applyDefaults()

	rc.process(spec, amqp.Delivery{Acknowledger: ack, DeliveryTag: 1})

	if !ok {
		t.Fatal("handler context had no deadline")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 5*time.Second {
		t.Errorf("deadline %v out of range for a 5s timeout", remaining)
	}
}

func TestApplyDefaults(t *testing.T) {
	t.Run("fills zero values", func(t *testing.T) {
		cfg := ConsumerConfig{Queue: "q"}
		cfg.applyDefaults()

		if cfg.Prefetch != defaultPrefetch {
			t.Errorf("Prefetch = %d, want %d", cfg.Prefetch, defaultPrefetch)
		}
		if cfg.Workers != defaultWorkers {
			t.Errorf("Workers = %d, want %d", cfg.Workers, defaultWorkers)
		}
		if cfg.Timeout != defaultTimeout {
			t.Errorf("Timeout = %v, want %v", cfg.Timeout, defaultTimeout)
		}
	})

	t.Run("workers never exceed prefetch", func(t *testing.T) {
		cfg := ConsumerConfig{Queue: "q", Prefetch: 2, Workers: 10}
		cfg.applyDefaults()

		if cfg.Workers != 2 {
			t.Errorf("Workers = %d, want 2 (capped at prefetch)", cfg.Workers)
		}
	})

	t.Run("keeps explicit values", func(t *testing.T) {
		cfg := ConsumerConfig{Queue: "q", Prefetch: 8, Workers: 3, Timeout: time.Second}
		cfg.applyDefaults()

		if cfg.Prefetch != 8 || cfg.Workers != 3 || cfg.Timeout != time.Second {
			t.Errorf("defaults overwrote explicit config: %+v", cfg)
		}
	})
}
