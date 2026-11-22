package realmlifecycle

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func scopeDB(t *testing.T) *pgx.Conn {
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
	schema := fmt.Sprintf("realm_lifecycle_worker_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := db.Exec(ctx, `DROP SCHEMA "`+schema+`" CASCADE`); err != nil {
			t.Error(err)
		}
		db.Close(ctx)
	})
	_, err = db.Exec(t.Context(), `CREATE SCHEMA "`+schema+`"; SET search_path TO "`+schema+`";
	 CREATE TABLE entity_entity (id text PRIMARY KEY, type text DEFAULT 'realm');
	 CREATE TABLE community_realm (id text PRIMARY KEY, realm_id text UNIQUE, entity_id text REFERENCES entity_entity(id),
	 parent_id text REFERENCES community_realm(realm_id), type text, is_private boolean DEFAULT FALSE,
	 is_active boolean DEFAULT FALSE, deletion_requested_at timestamptz);
	 CREATE TABLE entity_role (id bigint PRIMARY KEY, realm_id text REFERENCES community_realm(id), code text, is_default boolean);
	 CREATE TABLE community_member (entity_id text REFERENCES entity_entity(id), realm_id text REFERENCES community_realm(id), role_id bigint REFERENCES entity_role(id));
	 CREATE TABLE newsfeed_post (post_id text PRIMARY KEY, entity_id text REFERENCES entity_entity(id));
	 CREATE TABLE newsfeed_posttag (post_id text REFERENCES newsfeed_post(post_id), entity_id text REFERENCES entity_entity(id));`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestScopePostgresBoundedContentAndCurrentOwnership(t *testing.T) {
	db := scopeDB(t)
	ctx := t.Context()
	stamp := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	_, err := db.Exec(ctx, `INSERT INTO entity_entity(id) VALUES ('community-entity'), ('page-entity'), ('person');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at)
	 VALUES ('community','community','community-entity','community',$1), ('page','page','page-entity','page',$1);
	 INSERT INTO entity_role VALUES (1,'community','owner',TRUE),(2,'community','member',TRUE);
	 INSERT INTO community_member VALUES ('page-entity','community',2);
	 INSERT INTO newsfeed_post VALUES ('outside-source','person'),('page-post','page-entity');
	 INSERT INTO newsfeed_posttag VALUES ('outside-source','page-entity');`, pgx.QueryExecModeSimpleProtocol, stamp)
	if err != nil {
		t.Fatal(err)
	}
	store := SQLStore{DB: db}
	page, err := store.LoadDeletionScope(ctx, DeletionJob{"page", stamp})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := store.PostBatch(ctx, page, "")
	if err != nil || len(ids) != 1 || ids[0] != "page-post" {
		t.Fatalf("page membership/tag must not delete an outside source: %v %v", ids, err)
	}
	if _, err := db.Exec(ctx, `UPDATE community_member SET role_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LoadDeletionScope(ctx, DeletionJob{"page", stamp}); !errors.Is(err, ErrOwnedRealms) {
		t.Fatalf("current page owner dependency ignored: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO newsfeed_post SELECT 'scoped-' || lpad(value::text,3,'0'), 'person' FROM generate_series(1,205) value;
	 INSERT INTO newsfeed_posttag SELECT post_id,'community-entity' FROM newsfeed_post WHERE post_id LIKE 'scoped-%'`); err != nil {
		t.Fatal(err)
	}
	scope, err := store.LoadDeletionScope(ctx, DeletionJob{"community", stamp})
	if err != nil {
		t.Fatal(err)
	}
	after, total := "", 0
	for {
		batch, err := store.PostBatch(ctx, scope, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) > BatchSize {
			t.Fatal("unbounded post load")
		}
		if len(batch) == 0 {
			break
		}
		total += len(batch)
		after = batch[len(batch)-1]
	}
	if total != 205 {
		t.Fatalf("wrong community content scope: %d", total)
	}
}

func TestInventoryPostgresRejectsNewUnclassifiedReference(t *testing.T) {
	db := scopeDB(t)
	store := SQLStore{DB: db}
	if err := store.CheckInventory(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(t.Context(), `CREATE TABLE newly_added_feature (realm_id text REFERENCES community_realm(id))`); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckInventory(t.Context()); !errors.Is(err, ErrUnknownReference) {
		t.Fatalf("unknown reference must stop cleanup: %v", err)
	}
}
