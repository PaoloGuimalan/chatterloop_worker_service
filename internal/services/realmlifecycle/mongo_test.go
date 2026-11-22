package realmlifecycle

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func mongoDB(t *testing.T) *mongo.Database {
	t.Helper()
	address := os.Getenv("REALM_LIFECYCLE_TEST_MONGO_URL")
	if address == "" {
		t.Skip("requires explicit disposable MongoDB fixture")
	}
	parsed, err := url.Parse(address)
	if err != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") || !strings.HasPrefix(parsed.Path, "/test_") {
		t.Fatal("only explicit local test_* MongoDB fixtures are allowed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(address))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		t.Fatal(err)
	}
	db := client.Database(fmt.Sprintf("test_realm_lifecycle_%d", time.Now().UnixNano()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := db.Drop(ctx); err != nil {
			t.Error(err)
		}
		client.Disconnect(ctx)
	})
	return db
}

func TestMongoPurgeOwnedInboxPreservesOtherCommunitiesAndReadState(t *testing.T) {
	db := mongoDB(t)
	ctx := t.Context()
	_, err := db.Collection("notifications").InsertMany(ctx, []interface{}{
		bson.M{"notificationID": "inbox", "toUserID": "community", "type": "community_join_request"},
		bson.M{"notificationID": "receipt", "toUserID": "admin", "type": "community_admin_inbox", "referenceID": "inbox"},
		bson.M{"notificationID": "preview", "toUserID": "admin", "fromUserID": "community", "type": "community_admin_activity", "referenceID": "community"},
		bson.M{"notificationID": "other", "toUserID": "other-community", "type": "community_post_created"},
		bson.M{"notificationID": "other-receipt", "toUserID": "admin", "type": "community_admin_inbox", "referenceID": "other", "isRead": true},
		bson.M{"notificationID": "personal", "toUserID": "admin", "fromUserID": "community", "type": "follow", "isRead": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := Store{Mongo: db}
	scope := DeletionScope{EntityIDs: []string{"community"}, CommunityEntityIDs: []string{"community"}}
	for i := 0; i < 2; i++ {
		if err := store.purgeOwnedNotifications(ctx, scope); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"inbox", "receipt", "preview"} {
		if count, err := db.Collection("notifications").CountDocuments(ctx, bson.M{"notificationID": id}); err != nil || count != 0 {
			t.Fatalf("scoped notification remains %s: %d %v", id, count, err)
		}
	}
	for _, id := range []string{"personal", "other-receipt"} {
		if count, err := db.Collection("notifications").CountDocuments(ctx, bson.M{"notificationID": id, "isRead": true}); err != nil || count != 1 {
			t.Fatalf("unrelated notification changed %s: %d %v", id, count, err)
		}
	}
	if count, err := db.Collection("notifications").CountDocuments(ctx, bson.M{"toUserID": "other-community"}); err != nil || count != 1 {
		t.Fatalf("another realm inbox changed: %d %v", count, err)
	}
}
