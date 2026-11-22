package realmlifecycle

import (
	"context"
	"errors"
	"strings"
	"testing"

	"worker_service/internal/services/media"
)

type releaseBucket struct {
	fail    bool
	removed []string
}

func (b *releaseBucket) KeyFromURL(raw string) string {
	if strings.HasPrefix(raw, "https://media.fixture/") {
		return strings.TrimPrefix(raw, "https://media.fixture/")
	}
	return ""
}
func (b *releaseBucket) Remove(_ context.Context, key string) error {
	if b.fail {
		return errors.New("storage unavailable")
	}
	b.removed = append(b.removed, key)
	return nil
}
func (*releaseBucket) AbortMultipart(context.Context, string, string) error { return nil }

type releaseStore struct{ shared bool }

func (*releaseStore) FindRecord(context.Context, string, string) (*media.FileRecord, error) {
	return nil, nil
}
func (*releaseStore) SaveRecord(context.Context, *media.FileRecord) error      { return nil }
func (*releaseStore) IsReported(context.Context, media.Target) (bool, error)   { return false, nil }
func (*releaseStore) IsTargetLive(context.Context, media.Target) (bool, error) { return false, nil }
func (s *releaseStore) URLInUse(context.Context, string, string, *media.Target, string) (bool, error) {
	return s.shared, nil
}

func TestDeletionMediaPostgresRetainsReferencesAcrossStorageFailure(t *testing.T) {
	db := scopeDB(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `ALTER TABLE community_realm ADD COLUMN profile text; ALTER TABLE community_realm ADD COLUMN cover_photo text;
	 ALTER TABLE newsfeed_post ADD COLUMN details jsonb;
	 CREATE TABLE newsfeed_postreference (reference_id text, post_id text, reference text);
	 CREATE TABLE newsfeed_comment (comment_id text, post_id text, entity_id text, attachment text);
	 CREATE TABLE notes_entry (id uuid, entity_id text);
	 CREATE TABLE notes_attachment (id uuid, note_id uuid, url text);
	 INSERT INTO entity_entity(id) VALUES ('realm-entity'),('other-entity');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,profile) VALUES
	 ('realm','realm','realm-entity','community','https://media.fixture/uploads/communities/realm/profile.jpg'),
	 ('outside','outside','other-entity','community','https://media.fixture/uploads/communities/outside/profile.jpg');`)
	if err != nil {
		t.Fatal(err)
	}
	scope := DeletionScope{RealmIDs: []string{"realm"}, EntityIDs: []string{"realm-entity"}, CommunityEntityIDs: []string{"realm-entity"}}
	bucket := &releaseBucket{fail: true}
	store := Store{SQLStore: SQLStore{DB: db}, Bucket: bucket, MediaStore: &releaseStore{}}
	if err := store.releaseSQLMedia(ctx, scope); err == nil {
		t.Fatal("storage failure swallowed")
	}
	var profile string
	if err := db.QueryRow(ctx, `SELECT profile FROM community_realm WHERE id='realm'`).Scan(&profile); err != nil || profile == "" {
		t.Fatalf("lost media source before terminal outcome: %q %v", profile, err)
	}
	bucket.fail = false
	if err := store.releaseSQLMedia(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if len(bucket.removed) != 1 || bucket.removed[0] != "uploads/communities/realm/profile.jpg" {
		t.Fatalf("wrong media scope: %v", bucket.removed)
	}
	store.Bucket = nil
	if err := store.releaseSQLMedia(ctx, scope); !errors.Is(err, ErrDependency) {
		t.Fatalf("unconfigured storage forgotten: %v", err)
	}
	store.Bucket, store.MediaStore = bucket, &releaseStore{shared: true}
	if err := store.releaseSQLMedia(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if len(bucket.removed) != 1 {
		t.Fatal("shared media deleted")
	}
}
