package realmlifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"worker_service/internal/services/media"

	"github.com/gocql/gocql"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	ErrDependency  = errors.New("realm cleanup dependency unavailable")
	ErrCallsActive = errors.New("waiting for realm call shutdown")
)

type roomStore interface {
	Publish(context.Context, string, interface{}) *redis.IntCmd
	Exists(context.Context, ...string) *redis.IntCmd
	Del(context.Context, ...string) *redis.IntCmd
}

type Store struct {
	SQLStore
	Mongo      *mongo.Database
	Redis      roomStore
	Cassandra  *gocql.Session
	indexes    indexStore // Inert Cassandra adapter for integration fixtures.
	Bucket     media.Bucket
	MediaStore media.Store // Optional fixture adapter; production uses existing media.DBStore.
}

func (s Store) StopCalls(ctx context.Context, scope DeletionScope) error {
	if s.Redis == nil {
		return fmt.Errorf("%w: Redis", ErrDependency)
	}
	frame, err := json.Marshal(map[string]any{"event": "realm-lifecycle-relay", "message": map[string]string{"realm_id": scope.Job.RealmID}})
	if err != nil {
		return err
	}
	if err := s.Redis.Publish(ctx, "SUB_realm_lifecycle", string(frame)).Err(); err != nil {
		return err
	}
	var hostKeys, roomKeys []string
	for _, id := range scope.ConversationIDs {
		hostKeys = append(hostKeys, "call:room:pod:"+id)
		for _, prefix := range []string{"call:room:pod:", "call:members:", "call:status:", "call:producers:", "call:pipes:", "call:participants:", "call:invited:"} {
			roomKeys = append(roomKeys, prefix+id)
		}
	}
	if len(hostKeys) == 0 {
		return nil
	}
	active, err := s.Redis.Exists(ctx, hostKeys...).Result()
	if err != nil {
		return err
	}
	if active > 0 {
		return ErrCallsActive
	}
	return s.Redis.Del(ctx, roomKeys...).Err()
}

func (s Store) PrepareContent(ctx context.Context, scope DeletionScope) error {
	if s.Mongo == nil {
		return fmt.Errorf("%w: MongoDB", ErrDependency)
	}
	// Capture notification references before removing reaction rows. Anonymous
	// notices deliberately do not store the real actor in fromUserID.
	if err := s.purgeActorActivityNotices(ctx, scope); err != nil {
		return err
	}
	if err := s.SQLStore.PrepareContent(ctx, scope); err != nil {
		return err
	}
	_, err := s.Mongo.Collection("messages").UpdateMany(ctx,
		bson.M{"conversationID": bson.M{"$in": scope.ConversationIDs}, "isDeleted": bson.M{"$ne": true}},
		bson.M{"$set": bson.M{"isDeleted": true}})
	return err
}
