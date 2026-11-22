package realmlifecycle

import (
	"context"
	"time"
)

type progressKey struct{}

// A large healthy cleanup may outlive the queue's ordinary request deadline.
// Only completed datastore work renews this lease; an idle dependency still
// cancels the job, leaving its committed marker for reconciliation.
func WithProgressLease(parent context.Context, idle time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	pulses := make(chan struct{}, 1)
	ctx = context.WithValue(ctx, progressKey{}, pulses)
	go func() {
		timer := time.NewTimer(idle)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-pulses:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(idle)
			case <-timer.C:
				cancel()
				return
			}
		}
	}()
	return ctx, cancel
}
func Progress(ctx context.Context) {
	if pulses, ok := ctx.Value(progressKey{}).(chan struct{}); ok {
		select {
		case pulses <- struct{}{}:
		default:
		}
	}
}
