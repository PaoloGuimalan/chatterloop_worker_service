package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"worker_service/internal/connections"
	"worker_service/internal/services/media"
	"worker_service/internal/services/realmlifecycle"
)

func DeleteRealm(ctx context.Context, job realmlifecycle.DeletionJob) error {
	ctx, cancel := realmlifecycle.WithProgressLease(ctx, 5*time.Minute)
	defer cancel()
	if job.RealmID == "" || job.RequestedAt.IsZero() {
		return fmt.Errorf("%w: invalid deletion intent", ErrDrop)
	}
	store := realmlifecycle.Store{SQLStore: realmlifecycle.SQLStore{DB: connections.Pool()},
		Cassandra: connections.CassandraSession(), Bucket: media.DefaultStorage()}
	if collection := connections.Collection("messages"); collection != nil {
		store.Mongo = collection.Database()
	}
	// Do not assign a typed nil Redis client to the interface.
	if client := connections.RedisClient(); client != nil {
		store.Redis = client
	}
	err := realmlifecycle.HandleDeletion(ctx, store, job)
	if errors.Is(err, realmlifecycle.ErrIntentMismatch) {
		return fmt.Errorf("%w: %v", ErrDrop, err)
	}
	return err
}
