package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"
	"worker_service/internal/connections"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
)

const communityFanoutType = "community"
const fanoutBatchSize = 100

type fanoutScope struct {
	AuthorID, Kind, RealmID, CommunityID string
	Live, Anonymous, Private             bool
	PostedAt                             time.Time
}

type fanoutStore interface {
	postScope(context.Context, string) (fanoutScope, error)
	commentActor(context.Context, string, string) (string, bool, time.Time, error)
	recipientPage(context.Context, fanoutScope, string, string, int) ([]string, error)
}

type feedIndexRow struct {
	RecipientID, PostID, AuthorID, Type, TriggeredBy, Kind string
}

func (s fanoutScope) allowsFollowerFanout() bool {
	// Community-authored system media updates are intended for members only.
	return !s.Anonymous && (s.CommunityID == "" || (!s.Private && s.AuthorID != s.CommunityID))
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type communityPushStore interface {
	postScope(context.Context, string) (fanoutScope, error)
	currentMembers(context.Context, string, []string) ([]string, error)
}

func prepareCommunityPush(ctx context.Context, store communityPushStore, payload SendPushPayload) (SendPushPayload, error) {
	if payload.CommunityPostID == "" {
		return payload, nil
	}
	if store == nil {
		return SendPushPayload{}, fmt.Errorf("postgres is not connected")
	}
	scope, err := store.postScope(ctx, payload.CommunityPostID)
	if err != nil {
		return SendPushPayload{}, err
	}
	if !scope.Live || scope.CommunityID == "" {
		return SendPushPayload{}, fmt.Errorf("community post is unavailable")
	}
	ids, err := store.currentMembers(ctx, scope.RealmID, payload.EntityIDs)
	if err != nil {
		return SendPushPayload{}, err
	}
	payload.EntityIDs, payload.Tokens, payload.ImageURL = ids, nil, ""
	payload.Title, payload.Body = "Community activity", "New activity in your community."
	payload.Tag = "community-" + payload.CommunityPostID
	payload.Channel = ChannelActivity
	route := "/post/" + url.PathEscape(payload.CommunityPostID)
	if parsed, err := url.Parse(payload.Data["route"]); err == nil && parsed.Path == route {
		if anchor := parsed.Query().Get("anchor"); anchor != "" {
			route += "?anchor=" + url.QueryEscape(anchor)
		}
	}
	payload.Data = map[string]string{"type": "community_activity", "title": payload.Title, "body": payload.Body, "route": route}
	return payload, nil
}

func communityChatAllowed(ctx context.Context, db rowQuerier, conversationID, actorID string) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("postgres is not connected")
	}
	var community bool
	err := db.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM community_realm WHERE type = 'community'
		AND (realm_id = $1 OR id = $1 OR entity_id::text = $1 OR entity_id::text = $2)
	)`, conversationID, actorID).Scan(&community)
	if err != nil {
		return false, err
	}
	return !community, nil
}

// Each delivery writes at most one page. Its original checkpoint remains on the
// broker until both the batch and its durable continuation succeed. Replaying a
// page after partial Cassandra writes or an uncertain publish is idempotent.
func runPostFanout(ctx context.Context, store fanoutStore, job BulkFanoutPayload, emit func(context.Context, time.Time, []feedIndexRow) error, enqueue func(context.Context, BulkFanoutPayload) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, err := store.postScope(ctx, job.PostData.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil // The post was removed before this delivery.
	}
	if err != nil {
		return err
	}
	if !scope.Live {
		return nil
	}
	rowType := job.Type
	if rowType == "" {
		rowType = DefaultFanoutType
	}
	followerID := scope.AuthorID
	at := scope.PostedAt
	if rowType == CommentFanoutType {
		followerID = job.CurrentEntityID
		if job.CommentID != "" {
			var anonymous bool
			followerID, anonymous, at, err = store.commentActor(ctx, job.PostData.ID, job.CommentID)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // A removed comment must not resurface a post.
			}
			if err != nil {
				return err
			}
			if anonymous {
				return nil
			}
		} else if scope.CommunityID != "" {
			// Older jobs cannot prove a community comment's actor or anonymity.
			followerID = ""
		}
	}
	triggeredBy := followerID
	if scope.CommunityID != "" && (rowType != CommentFanoutType || followerID == "") {
		rowType, triggeredBy = communityFanoutType, scope.CommunityID
	}
	if !scope.allowsFollowerFanout() {
		followerID = ""
	}
	// Legacy publishers need no new fields: their stable timestamp comes from
	// the stored activity. Never use processing time as part of a retry's key.
	if !job.IndexedAt.IsZero() {
		at = job.IndexedAt
	}
	if at.IsZero() {
		return fmt.Errorf("%w: fanout activity timestamp is missing", ErrDrop)
	}
	ids, err := store.recipientPage(ctx, scope, followerID, job.AfterRecipient, fanoutBatchSize)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	next := ids[len(ids)-1]
	if next <= job.AfterRecipient {
		return fmt.Errorf("%w: fanout recipient cursor did not advance", ErrDrop)
	}
	rows := make([]feedIndexRow, 0, len(ids))
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		rows = append(rows, feedIndexRow{RecipientID: id, PostID: job.PostData.ID, AuthorID: scope.AuthorID, Type: rowType, TriggeredBy: triggeredBy, Kind: scope.Kind})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(rows) > 0 {
		if err := emit(ctx, at, rows); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ids) < fanoutBatchSize {
		return nil
	}
	job.AfterRecipient, job.IndexedAt = next, at
	return enqueue(ctx, job)
}

type fanoutDB interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type postgresFanoutStore struct{ db fanoutDB }

func (s postgresFanoutStore) currentMembers(ctx context.Context, realmID string, ids []string) ([]string, error) {
	rows, err := s.db.Query(ctx, `SELECT DISTINCT entity_id::text FROM community_member
		WHERE realm_id = $1 AND entity_id::text = ANY($2::text[]) ORDER BY entity_id::text`, realmID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		members = append(members, id)
	}
	return members, rows.Err()
}

func (s postgresFanoutStore) postScope(ctx context.Context, postID string) (fanoutScope, error) {
	var scope fanoutScope
	var destinations int
	err := s.db.QueryRow(ctx, `
		SELECT p.entity_id::text, p.on_feed,
		       p.deleted_at IS NULL AND (p.expires_at IS NULL OR p.expires_at > now())
		       AND coalesce(c.active, true), p.is_anonymous,
		       coalesce(c.realm_id, ''), coalesce(c.entity_id, ''),
		       coalesce(c.is_private, false), c.destinations, p.date_posted
		FROM newsfeed_post p
		LEFT JOIN LATERAL (
		    SELECT min(r.id) AS realm_id, min(r.entity_id::text) AS entity_id,
		           bool_or(r.is_private) AS is_private,
		           bool_and(r.is_active AND (r.expires_at IS NULL OR r.expires_at > now())) AS active,
		           count(DISTINCT r.id) AS destinations
		    FROM newsfeed_posttag t JOIN community_realm r ON r.entity_id = t.entity_id
		    WHERE t.post_id = p.post_id AND r.type = 'community'
		) c ON true
		WHERE p.post_id = $1`, postID).Scan(&scope.AuthorID, &scope.Kind, &scope.Live, &scope.Anonymous, &scope.RealmID, &scope.CommunityID, &scope.Private, &destinations, &scope.PostedAt)
	if err != nil {
		return fanoutScope{}, err
	}
	if destinations > 1 {
		return fanoutScope{}, fmt.Errorf("%w: post has multiple community destinations", ErrDrop)
	}
	scope.Kind = normalizePostKind(scope.Kind)
	if scope.CommunityID != "" && scope.Kind != PostKindFeed {
		return fanoutScope{}, fmt.Errorf("%w: community destination requires a feed post", ErrDrop)
	}
	return scope, nil
}

func (s postgresFanoutStore) commentActor(ctx context.Context, postID, commentID string) (string, bool, time.Time, error) {
	var actor string
	var anonymous bool
	var at time.Time
	err := s.db.QueryRow(ctx, `SELECT entity_id::text, is_anonymous, created_at FROM newsfeed_comment
		WHERE comment_id = $1 AND post_id = $2 AND deleted_at IS NULL`, commentID, postID).Scan(&actor, &anonymous, &at)
	return actor, anonymous, at, err
}

func (s postgresFanoutStore) recipientPage(ctx context.Context, scope fanoutScope, followerID, after string, limit int) ([]string, error) {
	// UNION and a keyset cursor deduplicate members/followers without retaining
	// the community's entire membership in memory. Ordinary followers retain
	// their existing top-500 behavior; only community membership is uncapped.
	rows, err := s.db.Query(ctx, `
		WITH followers AS (
		    SELECT follower_id::text AS recipient FROM entity_follow
		    WHERE followee_id = $2 AND status = true AND $2 <> ''
		    ORDER BY interaction_score DESC, last_interaction_at DESC, follower_id
		    LIMIT 500
		), recipients AS (
		    SELECT entity_id::text COLLATE "C" AS recipient FROM community_member
		    WHERE realm_id = $1 AND $1 <> ''
		    UNION SELECT recipient COLLATE "C" FROM followers
		)
		SELECT recipient FROM recipients WHERE recipient > $3 COLLATE "C"
		ORDER BY recipient LIMIT $4`, scope.RealmID, followerID, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func writeFanoutBatch(ctx context.Context, at time.Time, rows []feedIndexRow) error {
	session := connections.CassandraSession()
	if session == nil {
		return fmt.Errorf("Astra DB disconnected")
	}
	batch := session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	for _, row := range rows {
		batch.Query(`INSERT INTO newsfeed_index (bucket, post_id, created_at, author_id, type, triggered_by, kind)
			VALUES (?, ?, ?, ?, ?, ?, ?)`, row.RecipientID, row.PostID, at, row.AuthorID, row.Type, row.TriggeredBy, row.Kind)
	}
	return session.ExecuteBatch(batch)
}
