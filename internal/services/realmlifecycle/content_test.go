package realmlifecycle

import (
	"testing"
	"time"
)

func TestPrepareContentPostgresIsScopedAndCountsOnlyOnce(t *testing.T) {
	db := scopeDB(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `ALTER TABLE newsfeed_post ADD COLUMN deleted_at timestamptz;
	 CREATE TABLE newsfeed_comment (comment_id text PRIMARY KEY, post_id text REFERENCES newsfeed_post(post_id),
	 parent_comment_id text REFERENCES newsfeed_comment(comment_id), entity_id text, text text, deleted_at timestamptz);
	 CREATE TABLE newsfeed_postscore (post_id text PRIMARY KEY, comments_count int, likes_count int,
	 affinity_score float,content_type_weight float,recent_update_boost float,shares_count int,ranking_score float);
	 CREATE TABLE newsfeed_activitycount (post_id text, count_type text, count int);
	 CREATE TABLE newsfeed_reaction (reaction_id text PRIMARY KEY, post_id text, entity_id text, emoji_id text);
	 CREATE TABLE newsfeed_previewcount (post_id text, emoji_id text, count int);
	 CREATE TABLE newsfeed_commentreaction (reaction_id text PRIMARY KEY, comment_id text, entity_id text, emoji_id text);
	 CREATE TABLE newsfeed_commentpreviewcount (comment_id text, emoji_id text, count int);
	 INSERT INTO entity_entity(id) VALUES ('page-entity'), ('person');
	 INSERT INTO newsfeed_post VALUES ('owned','page-entity',NULL), ('outside','person',NULL);
	 INSERT INTO newsfeed_comment VALUES ('parent','outside',NULL,'page-entity','Page reply',NULL),
	 ('child','outside','parent','person','Other reply',NULL);
	 INSERT INTO newsfeed_reaction VALUES ('own-react','outside','page-entity','like'),('other-react','outside','person','like');
	 INSERT INTO newsfeed_commentreaction VALUES ('own-comment-react','child','page-entity','like'),('other-comment-react','child','person','like');
	 INSERT INTO newsfeed_postscore(post_id,comments_count,likes_count) VALUES ('outside',2,2);
	 INSERT INTO newsfeed_activitycount VALUES ('outside','comment',2);
	 INSERT INTO newsfeed_previewcount VALUES ('outside','like',2);
	 INSERT INTO newsfeed_commentpreviewcount VALUES ('child','like',2);`)
	if err != nil {
		t.Fatal(err)
	}
	scope := DeletionScope{Job: DeletionJob{"page", time.Now().UTC()}, EntityIDs: []string{"page-entity"}}
	store := SQLStore{DB: db}
	for i := 0; i < 2; i++ {
		if err := store.PrepareContent(ctx, scope); err != nil {
			t.Fatal(err)
		}
	}
	var comments, likes, preview, commentPreview, activity int
	err = db.QueryRow(ctx, `SELECT s.comments_count,s.likes_count,p.count,c.count,a.count
	 FROM newsfeed_postscore s CROSS JOIN newsfeed_previewcount p
	 CROSS JOIN newsfeed_commentpreviewcount c CROSS JOIN newsfeed_activitycount a`).Scan(&comments, &likes, &preview, &commentPreview, &activity)
	if err != nil || comments != 1 || likes != 1 || preview != 1 || commentPreview != 1 || activity != 1 {
		t.Fatalf("retries changed counters incorrectly: %d/%d/%d/%d/%d %v", comments, likes, preview, commentPreview, activity, err)
	}
	var liveOutside, deletedOwned, childSurvives bool
	err = db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newsfeed_post WHERE post_id='outside' AND deleted_at IS NULL),
	 EXISTS(SELECT 1 FROM newsfeed_post WHERE post_id='owned' AND deleted_at IS NOT NULL),
	 EXISTS(SELECT 1 FROM newsfeed_comment WHERE comment_id='child' AND deleted_at IS NULL AND text='Other reply')`).Scan(&liveOutside, &deletedOwned, &childSurvives)
	if err != nil || !liveOutside || !deletedOwned || !childSurvives {
		t.Fatalf("cleanup changed outside content: %v/%v/%v %v", liveOutside, deletedOwned, childSurvives, err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO newsfeed_post VALUES ('new-outside','person',NULL);
	 INSERT INTO newsfeed_comment VALUES ('late','new-outside',NULL,'page-entity','Late page comment',NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := store.PrepareContent(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(ctx, `SELECT comments_count FROM newsfeed_postscore WHERE post_id='new-outside'`).Scan(&comments); err != nil || comments != 0 {
		t.Fatalf("cleanup did not serialize a missing score row: %d %v", comments, err)
	}
}
