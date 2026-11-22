package realmlifecycle

import "testing"

func TestPurgePostsPostgresPreservesOutsideSourcesReportsAndExistingShareCounts(t *testing.T) {
	db := scopeDB(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `ALTER TABLE newsfeed_post ADD COLUMN deleted_at timestamptz;
	 ALTER TABLE newsfeed_post ADD COLUMN is_shared boolean DEFAULT FALSE;
	 CREATE TABLE newsfeed_postreference(reference_id text,post_id text REFERENCES newsfeed_post(post_id),reference text,reference_media_type text);
	 CREATE TABLE entity_report(target_type text,target_id text);
	 CREATE TABLE newsfeed_comment(comment_id text PRIMARY KEY,post_id text REFERENCES newsfeed_post(post_id),entity_id text,text text,attachment text,deleted_at timestamptz);
	 CREATE TABLE newsfeed_commentreaction(comment_id text REFERENCES newsfeed_comment(comment_id));
	 CREATE TABLE newsfeed_commentpreviewcount(comment_id text REFERENCES newsfeed_comment(comment_id));
	 CREATE TABLE newsfeed_postscore(post_id text PRIMARY KEY REFERENCES newsfeed_post(post_id),shares_count int);
	 CREATE TABLE newsfeed_activitycount(post_id text REFERENCES newsfeed_post(post_id),count_type text,count int);
	 CREATE TABLE newsfeed_mapview(post_id text REFERENCES newsfeed_post(post_id));
	 CREATE TABLE newsfeed_reaction(post_id text REFERENCES newsfeed_post(post_id));
	 CREATE TABLE newsfeed_previewcount(post_id text REFERENCES newsfeed_post(post_id));
	 CREATE TABLE newsfeed_postprivacy(post_id text REFERENCES newsfeed_post(post_id));
	 CREATE TABLE newsfeed_postsave(post_id text REFERENCES newsfeed_post(post_id));
	 CREATE TABLE interests_postinterestlink(post_id text REFERENCES newsfeed_post(post_id));
	 INSERT INTO entity_entity(id) VALUES ('page-entity'),('person');
	 INSERT INTO newsfeed_post VALUES ('source','person',NULL,FALSE),('share','page-entity',NOW(),TRUE),('reported','page-entity',NOW(),FALSE);
	 INSERT INTO newsfeed_postreference VALUES ('ref','share','source','post');
	 INSERT INTO newsfeed_postscore VALUES ('source',2),('share',0);
	 INSERT INTO newsfeed_activitycount VALUES ('source','share',2),('share','share',0);
	 INSERT INTO newsfeed_comment VALUES ('child','share','person','Reply','https://media/file',NOW());
	 INSERT INTO newsfeed_commentreaction VALUES ('child'); INSERT INTO newsfeed_commentpreviewcount VALUES ('child');
	 INSERT INTO entity_report VALUES ('post','reported');`)
	if err != nil {
		t.Fatal(err)
	}
	store := SQLStore{DB: db}
	scope := DeletionScope{EntityIDs: []string{"page-entity"}}
	for i := 0; i < 2; i++ {
		if err := store.PurgePosts(ctx, scope); err != nil {
			t.Fatal(err)
		}
	}
	var source, share, reported bool
	var score, activity int
	err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newsfeed_post WHERE post_id='source' AND deleted_at IS NULL),
	 EXISTS(SELECT 1 FROM newsfeed_post WHERE post_id='share'),EXISTS(SELECT 1 FROM newsfeed_post WHERE post_id='reported' AND deleted_at IS NOT NULL),
	 (SELECT shares_count FROM newsfeed_postscore WHERE post_id='source'),(SELECT count FROM newsfeed_activitycount WHERE post_id='source')`).Scan(&source, &share, &reported, &score, &activity)
	if err != nil || !source || share || !reported || score != 2 || activity != 2 {
		t.Fatalf("wrong scoped purge: %v/%v/%v %d/%d %v", source, share, reported, score, activity, err)
	}
}
