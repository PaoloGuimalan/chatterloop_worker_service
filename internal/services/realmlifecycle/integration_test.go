package realmlifecycle

import (
	"context"
	"os"
	"testing"
	"time"

	"worker_service/internal/connections"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
)

type quietIndexes struct{}

func (quietIndexes) Iter(context.Context, string, ...any) cqlRows       { return &indexRowsFixture{} }
func (quietIndexes) DeleteBatch(context.Context, string, [][]any) error { return nil }

func TestDeletionIntegrationPostgresMongoAndExistingMediaPolicy(t *testing.T) {
	db := scopeDB(t)
	completeInventoryFixture(t, db)
	mongodb := mongoDB(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `ALTER TABLE community_realm ADD COLUMN profile text; ALTER TABLE community_realm ADD COLUMN cover_photo text;
	 ALTER TABLE user_account ADD COLUMN profile text; ALTER TABLE user_account ADD COLUMN coverphoto text;
	 ALTER TABLE newsfeed_post ADD COLUMN details jsonb; ALTER TABLE newsfeed_post ADD COLUMN is_shared boolean DEFAULT FALSE;
	 ALTER TABLE newsfeed_postreference ADD COLUMN reference_id text; ALTER TABLE newsfeed_postreference ADD COLUMN reference text;
	 ALTER TABLE newsfeed_postreference ADD COLUMN reference_media_type text;
	 ALTER TABLE newsfeed_comment ADD COLUMN text text; ALTER TABLE newsfeed_comment ADD COLUMN attachment text; ALTER TABLE newsfeed_comment ADD COLUMN deleted_at timestamptz;
	 ALTER TABLE newsfeed_postscore ADD COLUMN comments_count int DEFAULT 0; ALTER TABLE newsfeed_postscore ADD COLUMN likes_count int DEFAULT 0;
	 ALTER TABLE newsfeed_postscore ALTER COLUMN id SET DEFAULT gen_random_uuid()::text;
	 ALTER TABLE newsfeed_postscore ADD COLUMN affinity_score float; ALTER TABLE newsfeed_postscore ADD COLUMN content_type_weight float;
	 ALTER TABLE newsfeed_postscore ADD COLUMN recent_update_boost float; ALTER TABLE newsfeed_postscore ADD COLUMN shares_count int;
	 ALTER TABLE newsfeed_postscore ADD COLUMN ranking_score float; CREATE UNIQUE INDEX score_post ON newsfeed_postscore(post_id);
	 ALTER TABLE newsfeed_reaction ADD COLUMN reaction_id text; ALTER TABLE newsfeed_reaction ADD COLUMN emoji_id text;
	 ALTER TABLE newsfeed_previewcount ADD COLUMN emoji_id text; ALTER TABLE newsfeed_previewcount ADD COLUMN count int;
	 ALTER TABLE newsfeed_activitycount ADD COLUMN count_type text; ALTER TABLE newsfeed_activitycount ADD COLUMN count int;
	 ALTER TABLE newsfeed_commentreaction ADD COLUMN reaction_id text; ALTER TABLE newsfeed_commentreaction ADD COLUMN emoji_id text;
	 ALTER TABLE newsfeed_commentpreviewcount ADD COLUMN emoji_id text; ALTER TABLE newsfeed_commentpreviewcount ADD COLUMN count int;
	 ALTER TABLE notes_attachment ADD COLUMN url text;
	 CREATE TABLE diary_attachment (url text);`)
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Truncate(time.Microsecond)
	entity := "11111111-1111-4111-8111-111111111111"
	person := "22222222-2222-4222-8222-222222222222"
	sharedURL := "https://media.fixture/uploads/communities/realm/shared.jpg"
	logoURL := "https://media.fixture/uploads/communities/realm/logo.jpg"
	_, err = db.Exec(ctx, `INSERT INTO entity_entity VALUES ($1,'realm'),($2,'user');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at,created_by_id,profile)
	 VALUES ('realm','realm',$1,'community',$3,$2,$4);
	 INSERT INTO entity_role VALUES (1,'realm','owner',TRUE);
	 INSERT INTO community_member(entity_id,realm_id,role_id,added_by_id) VALUES ($2,'realm',1,$2);
	 INSERT INTO user_account(id,entity_id,profile) VALUES ('account',$2,$5);
	 INSERT INTO newsfeed_post(post_id,entity_id) VALUES ('outside',$2),('inside',$2),('own',$1);
	 INSERT INTO newsfeed_posttag(post_id,entity_id) VALUES ('inside',$1);
	 INSERT INTO newsfeed_postreference(id,reference_id,post_id,reference,reference_media_type) VALUES ('ref','ref','own',$5,'image/jpeg');
	 INSERT INTO community_invite(id,realm_id,created_by_id,target_entity_id,status) VALUES ('invite','realm',$2,$2,'pending');`,
		pgx.QueryExecModeSimpleProtocol, entity, person, stamp, logoURL, sharedURL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = mongodb.Collection("messages").InsertMany(ctx, []any{
		bson.M{"messageID": "reported-text", "conversationID": "realm", "sender": person, "messageType": "text", "content": "Evidence must survive"},
		bson.M{"messageID": "scoped-message", "conversationID": "realm", "sender": person, "messageType": "text", "content": "old scoped history"},
		bson.M{"messageID": "historical-message", "conversationID": "outside", "sender": entity, "messageType": "text", "content": "Preserved outside history"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(ctx, `INSERT INTO entity_report(id,target_type,target_id) VALUES ('report','message','reported-text')`); err != nil {
		t.Fatal(err)
	}
	_, err = mongodb.Collection("conversations").InsertMany(ctx, []any{bson.M{"conversationID": "realm"}, bson.M{"conversationID": "outside", "participant_ids": []string{person, entity}}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mongodb.Collection("chat_history").InsertMany(ctx, []any{bson.M{"conversationID": "realm", "entityID": person}, bson.M{"conversationID": "outside", "entityID": person}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mongodb.Collection("notifications").InsertMany(ctx, []any{
		bson.M{"notificationID": "inbox", "toUserID": entity, "type": "community_join_request"},
		bson.M{"notificationID": "receipt", "toUserID": person, "type": "community_admin_inbox", "referenceID": "inbox"},
		bson.M{"notificationID": "preview", "toUserID": person, "fromUserID": entity, "type": "community_admin_activity", "referenceID": entity},
		bson.M{"notificationID": "invite-notice", "toUserID": person, "type": "realm_invite", "referenceID": "invite"},
		bson.M{"notificationID": "personal", "toUserID": person, "type": "follow", "isRead": true},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mongodb.Collection("files").InsertOne(ctx, bson.M{"version": 2, "key": "uploads/communities/realm/shared.jpg", "status": "attached",
		"ownerEntity": entity, "fileDetails": bson.M{"data": sharedURL}, "attachedTo": bson.A{bson.M{"type": "post", "id": "own"}}})
	if err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := db.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(os.Getenv("REALM_LIFECYCLE_TEST_DB_URL"))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	previousSQL, previousMongo := connections.Active, connections.ActiveMongo
	connections.Active, connections.ActiveMongo = &connections.Postgres{Pool: pool}, &connections.Mongo{Client: mongodb.Client(), Database: mongodb}
	t.Cleanup(func() { connections.Active, connections.ActiveMongo = previousSQL, previousMongo; pool.Close() })
	bucket := &releaseBucket{}
	store := Store{SQLStore: SQLStore{DB: db}, Mongo: mongodb, Redis: &roomFixture{}, indexes: quietIndexes{}, Bucket: bucket}
	job := DeletionJob{"realm", stamp}
	if err := HandleDeletion(ctx, store, job); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM community_realm WHERE id='realm'`, 0},
		{`SELECT COUNT(*) FROM newsfeed_post WHERE post_id IN ('inside','own')`, 0},
		{`SELECT COUNT(*) FROM newsfeed_post WHERE post_id='outside' AND deleted_at IS NULL`, 1},
		{`SELECT COUNT(*) FROM user_account WHERE id='account'`, 1},
		{`SELECT COUNT(*) FROM entity_entity WHERE id='11111111-1111-4111-8111-111111111111'`, 1},
	} {
		var count int
		if err := db.QueryRow(ctx, check.query).Scan(&count); err != nil || count != check.want {
			t.Fatalf("%s: %d want %d %v", check.query, count, check.want, err)
		}
	}
	for _, collection := range []string{"messages", "conversations", "chat_history"} {
		want := int64(0)
		if collection == "messages" {
			want = 1
		}
		if count, err := mongodb.Collection(collection).CountDocuments(ctx, bson.M{"conversationID": "realm"}); err != nil || count != want {
			t.Fatalf("scoped Mongo content remains %s: %d %v", collection, count, err)
		}
		if count, err := mongodb.Collection(collection).CountDocuments(ctx, bson.M{"conversationID": "outside"}); err != nil || count != 1 {
			t.Fatalf("outside Mongo content changed %s: %d %v", collection, count, err)
		}
	}
	if count, err := mongodb.Collection("messages").CountDocuments(ctx, bson.M{"messageID": "reported-text", "content": "Evidence must survive", "isDeleted": true}); err != nil || count != 1 {
		t.Fatalf("reported text evidence lost: %d %v", count, err)
	}
	if count, err := mongodb.Collection("notifications").CountDocuments(ctx, bson.M{}); err != nil || count != 1 {
		t.Fatalf("wrong notification cleanup: %d %v", count, err)
	}
	if len(bucket.removed) != 1 || bucket.removed[0] != "uploads/communities/realm/logo.jpg" {
		t.Fatalf("shared or outside media changed: %v", bucket.removed)
	}
	if err := HandleDeletion(ctx, store, job); err != nil {
		t.Fatal(err)
	}
	if len(bucket.removed) != 1 {
		t.Fatal("duplicate completed job touched storage")
	}
}
