package rabbitmq

import (
	"context"
	"github.com/jackc/pgx/v5"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
	"worker_service/internal/models"
)

func TestApprovalScopePostgres(t *testing.T) {
	address := os.Getenv("COMMUNITY_APPROVAL_TEST_DATABASE_URL")
	if address == "" {
		t.Skip("requires explicit disposable PostgreSQL URL")
	}
	parsed, err := url.Parse(address)
	if err != nil || !strings.HasPrefix(parsed.Path, "/test_") || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost") {
		t.Fatal("approval SQL tests require a local test_ database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := pgx.Connect(ctx, address)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	// Connection-local temporary tables; no application schema or migrations.
	_, err = db.Exec(ctx, `
        CREATE TEMP TABLE community_realm (id text, entity_id text, type text, is_active boolean, is_private boolean, expires_at timestamptz);
        CREATE TEMP TABLE community_member (realm_id text, entity_id text);
        CREATE TEMP TABLE community_approvals (id uuid, realm_id text, target_id text, target_kind text, action text);
        CREATE TEMP TABLE newsfeed_posttag (post_id text, entity_id text);
        CREATE TEMP TABLE newsfeed_comment (comment_id text, post_id text, entity_id text, is_anonymous boolean DEFAULT false, created_at timestamptz DEFAULT now(), deleted_at timestamptz);
        CREATE TEMP TABLE entity_follow (follower_id text, followee_id text, status boolean, interaction_score float, last_interaction_at timestamptz);
        CREATE TEMP TABLE newsfeed_post (
            post_id text, entity_id text, on_feed text DEFAULT 'feed', deleted_at timestamptz,
            expires_at timestamptz, is_anonymous boolean DEFAULT false, date_posted timestamptz DEFAULT now(),
            approval smallint DEFAULT 1, is_shared boolean DEFAULT false, file_type text DEFAULT 'text',
            caption text, content_type text DEFAULT 'text', is_tagged boolean DEFAULT false, privacy_status text DEFAULT 'public',
            is_sponsored boolean DEFAULT false, is_live boolean DEFAULT false, is_archived boolean DEFAULT false,
            from_system boolean DEFAULT false, deleted_by_id text, future_column text);
        INSERT INTO community_realm VALUES ('realm','community','community',true,true,NULL);
        INSERT INTO community_member VALUES ('realm','member');
        INSERT INTO newsfeed_post(post_id,entity_id) VALUES ('community-post','author'), ('ordinary','author');
        INSERT INTO newsfeed_posttag VALUES ('community-post','community');
        INSERT INTO newsfeed_comment(comment_id,post_id,entity_id) VALUES ('comment','community-post','actor');`)
	if err != nil {
		t.Fatal(err)
	}
	store := postgresFanoutStore{db: db}
	decisionID := "cb48b3df-bd09-44b5-b218-b47f1afce820"
	if _, err := db.Exec(ctx, `INSERT INTO community_approvals VALUES ($1,'realm','community-post','post','reject')`, decisionID); err != nil {
		t.Fatal(err)
	}
	decisionJob := SendPushPayload{EntityIDs: []string{"author", "outsider"}, Tokens: []string{"forged-token"},
		Body: "Hidden caption", ImageURL: "private.jpg", Data: map[string]string{"type": "community_post_decision", "reviewDecisionID": decisionID, "route": "/post/community-post"}}
	decisionPush, err := prepareCommunityDecisionPush(ctx, db, decisionJob)
	if err != nil || len(decisionPush.EntityIDs) != 1 || decisionPush.EntityIDs[0] != "author" || len(decisionPush.Tokens) != 0 || decisionPush.ImageURL != "" || decisionPush.Body == "Hidden caption" {
		t.Fatalf("former-author decision push did not retain its narrow ownership guard: %+v %v", decisionPush, err)
	}
	if decisionPush.Data["route"] != "/realm-review/"+decisionID {
		t.Fatal("decision push must open the human decision")
	}
	if _, err := db.Exec(ctx, `DELETE FROM newsfeed_post WHERE post_id='community-post'`); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCommunityDecisionPush(ctx, db, decisionJob); err == nil {
		t.Fatal("hard-deleted ownership cannot be proven")
	}
	if _, err := db.Exec(ctx, `INSERT INTO newsfeed_post(post_id,entity_id) VALUES ('community-post','author')`); err != nil {
		t.Fatal(err)
	}
	for _, state := range []int{0, 1, 2} {
		if _, err := db.Exec(ctx, "UPDATE newsfeed_post SET approval=$1", state); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"ordinary", "community-post"} {
			scope, err := store.postScope(ctx, id)
			expected := id == "ordinary" || state == 1
			if err != nil || scope.Live != expected {
				t.Fatalf("id=%s state=%d scope=%+v err=%v", id, state, scope, err)
			}
		}
		for _, activity := range []string{"", "comment"} {
			writes := 0
			err = runAllFanout(ctx, store, "actor", PostData{ID: "community-post"}, activity, "comment", func(rows []feedIndexRow) error { writes += len(rows); return nil })
			if err != nil || (state != 1 && writes != 0) {
				t.Fatalf("delayed job state=%d writes=%d err=%v", state, writes, err)
			}
			if state == 1 && writes == 0 {
				t.Fatal("approved community fanout was suppressed")
			}
		}
		if state != 1 {
			if _, err := prepareCommunityPush(ctx, store, SendPushPayload{CommunityPostID: "community-post"}); err == nil {
				t.Fatal("hidden post push allowed")
			}
		}
	}
	rows, err := db.Query(ctx, rankingPostSQL, "ordinary")
	if err != nil {
		t.Fatal(err)
	}
	post, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[models.Post])
	if err != nil || post.PostID != "ordinary" {
		t.Fatalf("additive columns broke ranking decode: %v", err)
	}
	_, err = db.Exec(ctx, `CREATE TEMP TABLE newsfeed_postscore (
		post_id text PRIMARY KEY, affinity_score float, content_type_weight float, recent_update_boost float,
        likes_count int, comments_count int, shares_count int, ranking_score float);
        INSERT INTO newsfeed_postscore VALUES ('ordinary',1,5,2,7,3,4,99)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, initialPostScoreSQL, "ordinary", 1., 5., 1., 0, 0, 0, 1.); err != nil {
		t.Fatal(err)
	}
	var likes, shares int
	if err := db.QueryRow(ctx, "SELECT likes_count,shares_count FROM newsfeed_postscore WHERE post_id='ordinary'").Scan(&likes, &shares); err != nil {
		t.Fatal(err)
	}
	if likes != 7 || shares != 4 {
		t.Fatalf("retried initial-score job erased engagement: likes=%d shares=%d", likes, shares)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE newsfeed_postscore ADD COLUMN id bigserial"); err != nil {
		t.Fatal(err)
	}
	updateRankingScore(ctx, db, "ordinary", "refresh", false)
	var boost, rank float64
	if err := db.QueryRow(ctx, "SELECT shares_count,recent_update_boost,ranking_score FROM newsfeed_postscore WHERE post_id='ordinary'").Scan(&shares, &boost, &rank); err != nil {
		t.Fatal(err)
	}
	if shares != 4 || boost != 2 || rank == 99 || rank <= 0 {
		t.Fatalf("refresh changed engagement/boost or did not recalculate: shares=%d boost=%v rank=%v", shares, boost, rank)
	}
}
