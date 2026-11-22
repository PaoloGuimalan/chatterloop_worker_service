package rabbitmq

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func lifecycleDeliveryDB(t *testing.T) *pgx.Conn {
	t.Helper()
	address := os.Getenv("REALM_LIFECYCLE_TEST_DB_URL")
	if address == "" {
		t.Skip("requires explicit disposable PostgreSQL fixtures")
	}
	parsed, err := url.Parse(address)
	if err != nil || !strings.HasPrefix(parsed.Path, "/test_") || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") {
		t.Fatal("only explicit local test_* databases are allowed")
	}
	db, err := pgx.Connect(t.Context(), address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		db.Close(ctx)
	})
	_, err = db.Exec(t.Context(), `CREATE TEMP TABLE community_realm(id text,realm_id text,entity_id text,type text,
	 parent_id text,is_active boolean DEFAULT TRUE,deletion_requested_at timestamptz,is_private boolean DEFAULT FALSE,expires_at timestamptz);
	 CREATE TEMP TABLE newsfeed_post(post_id text,entity_id text,on_feed text DEFAULT 'feed',deleted_at timestamptz,expires_at timestamptz,
	 approval int DEFAULT 1,is_anonymous boolean DEFAULT FALSE,date_posted timestamptz DEFAULT NOW());
	 CREATE TEMP TABLE newsfeed_posttag(post_id text,entity_id text);
	 CREATE TEMP TABLE entity_entity(id text,type text);
	 INSERT INTO entity_entity VALUES ('person','user'),('page-e','realm'),('gone-page-e','realm');
	 CREATE TEMP TABLE newsfeed_comment(comment_id text,post_id text,entity_id text,is_anonymous boolean DEFAULT FALSE,created_at timestamptz DEFAULT NOW(),deleted_at timestamptz);
	 INSERT INTO community_realm(id,realm_id,entity_id,type) VALUES ('community','community','community-e','community'),('page','page','page-e','page'),
	 ('server','server','server-e','server'),('child','child','child-e','channel');
	 UPDATE community_realm SET parent_id='server' WHERE id='child';
	 INSERT INTO newsfeed_post(post_id,entity_id) VALUES ('ordinary','person'),('scoped','person'),('page-post','page-e');
	 INSERT INTO newsfeed_posttag VALUES ('scoped','community-e');`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestQueuedPostDeliveryRechecksAuthorAndDestinationLifecycle(t *testing.T) {
	db := lifecycleDeliveryDB(t)
	ctx := t.Context()
	store := postgresFanoutStore{db: db}
	for _, post := range []string{"ordinary", "scoped", "page-post"} {
		scope, err := store.postScope(ctx, post)
		if err != nil || !scope.Live {
			t.Fatalf("active post changed: %s %+v %v", post, scope, err)
		}
	}
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=FALSE WHERE id='page';UPDATE community_realm SET deletion_requested_at=NOW() WHERE id='community'`); err != nil {
		t.Fatal(err)
	}
	for _, post := range []string{"scoped", "page-post"} {
		scope, err := store.postScope(ctx, post)
		if err != nil || scope.Live {
			t.Fatalf("unavailable scope delivered: %s %+v %v", post, scope, err)
		}
	}
	if scope, err := store.postScope(ctx, "ordinary"); err != nil || !scope.Live {
		t.Fatalf("unrelated personal post changed: %+v %v", scope, err)
	}
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=TRUE,deletion_requested_at=NULL`); err != nil {
		t.Fatal(err)
	}
	for _, post := range []string{"scoped", "page-post"} {
		scope, err := store.postScope(ctx, post)
		if err != nil || !scope.Live {
			t.Fatalf("reactivated post changed: %s %+v %v", post, scope, err)
		}
	}
}

func TestQueuedJoinNoticeBlocksInactiveParentsAndPageActors(t *testing.T) {
	db := lifecycleDeliveryDB(t)
	ctx := t.Context()
	if _, err := db.Exec(ctx, `UPDATE community_realm SET is_active=FALSE WHERE id IN ('server','page')`); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"child", "person"}, {"personal-dm", "page-e"}, {"personal-dm", "gone-page-e"}} {
		if allowed, err := communityChatAllowed(ctx, db, pair[0], pair[1]); err != nil || allowed {
			t.Fatalf("unavailable notice allowed: %v %v %v", pair, allowed, err)
		}
	}
	if allowed, err := communityChatAllowed(ctx, db, "personal-dm", "person"); err != nil || !allowed {
		t.Fatalf("personal message changed: %v %v", allowed, err)
	}
}
