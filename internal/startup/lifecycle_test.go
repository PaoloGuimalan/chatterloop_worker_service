package startup

import (
	"errors"
	"testing"
	"worker_service/internal/services/rabbitmq"
)

func TestDeletionConsumerIsBoundedAndRejectsInvalidIntent(t *testing.T) {
	cfg := realmDeletionConsumer()
	if !cfg.UntilShutdown {
		t.Fatal("cleanup has a fixed whole-job timeout")
	}
	if cfg.Queue != "realm_deletion" || cfg.Workers != 1 || cfg.Prefetch != 1 || cfg.RetryFailures {
		t.Fatalf("unsafe deletion consumer: %+v", cfg)
	}
	for _, body := range []string{"{", "{}"} {
		if err := cfg.Handler(t.Context(), []byte(body)); !errors.Is(err, rabbitmq.ErrDrop) {
			t.Fatalf("invalid job: %v", err)
		}
	}
}
