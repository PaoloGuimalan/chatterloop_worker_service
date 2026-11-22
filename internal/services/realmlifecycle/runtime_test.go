package realmlifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

type roomFixture struct {
	hosted  bool
	deleted []string
}

func (r *roomFixture) Publish(context.Context, string, interface{}) *redis.IntCmd {
	return redis.NewIntResult(1, nil)
}
func (r *roomFixture) Exists(context.Context, ...string) *redis.IntCmd {
	if r.hosted {
		return redis.NewIntResult(1, nil)
	}
	return redis.NewIntResult(0, nil)
}
func (r *roomFixture) Del(_ context.Context, keys ...string) *redis.IntCmd {
	r.deleted = append(r.deleted, keys...)
	return redis.NewIntResult(int64(len(keys)), nil)
}

func TestDeletionWaitsForCallShutdownWithoutClearingOtherRooms(t *testing.T) {
	room := &roomFixture{hosted: true}
	store := Store{Redis: room}
	scope := DeletionScope{Job: DeletionJob{RealmID: "target"}, ConversationIDs: []string{"target", "child"}}
	if err := store.StopCalls(t.Context(), scope); !errors.Is(err, ErrCallsActive) {
		t.Fatalf("live native call ignored: %v", err)
	}
	if len(room.deleted) != 0 {
		t.Fatal("metadata removed before the native router closed")
	}
	room.hosted = false
	if err := store.StopCalls(t.Context(), scope); err != nil {
		t.Fatal(err)
	}
	for _, key := range room.deleted {
		if !strings.HasSuffix(key, ":target") && !strings.HasSuffix(key, ":child") {
			t.Fatalf("cleared another room's state: %s", key)
		}
	}
	if err := (Store{}).StopCalls(t.Context(), scope); !errors.Is(err, ErrDependency) {
		t.Fatalf("unavailable shutdown dependency swallowed: %v", err)
	}
}
