package realmlifecycle

import (
	"testing"
	"time"
)

func TestCleanupLeaseAllowsOngoingBatchesAndCancelsStalledWork(t *testing.T) {
	ctx, cancel := WithProgressLease(t.Context(), 500*time.Millisecond)
	defer cancel()
	for i := 0; i < 6; i++ {
		time.Sleep(110 * time.Millisecond)
		Progress(ctx)
		if ctx.Err() != nil {
			t.Fatal("healthy batches hit a whole-job deadline")
		}
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stalled cleanup was not cancelled")
	}
}
