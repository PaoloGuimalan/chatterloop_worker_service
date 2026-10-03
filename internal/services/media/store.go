package media

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"

	"worker_service/internal/connections"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// DBStore is the Store over the real databases: the Node server's `files`
// and `messages` collections in Mongo, everything else in Postgres.
type DBStore struct{}

func (DBStore) pool() (*pgxpool.Pool, error) {
	pool := connections.Pool()
	if pool == nil {
		return nil, errors.New("postgres not connected")
	}
	return pool, nil
}

func collection(name string) (*mongo.Collection, error) {
	c := connections.Collection(name)
	if c == nil {
		return nil, errors.New("mongo not connected")
	}
	return c, nil
}

func (DBStore) FindRecord(ctx context.Context, rawURL, key string) (*FileRecord, error) {
	files, err := collection("files")
	if err != nil {
		return nil, err
	}
	var record FileRecord
	err = files.FindOne(ctx, bson.M{"version": 2, "key": key}).Decode(&record)
	if err == nil {
		return &record, nil
	}
	if !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}

	// An older record: found by its URL, stored as written (possibly with raw
	// spaces) or encoded.
	candidates := []string{rawURL}
	if decoded, err := url.PathUnescape(rawURL); err == nil && decoded != rawURL {
		candidates = append(candidates, decoded)
	}
	candidates = append(candidates, strings.ReplaceAll(rawURL, " ", "%20"))
	err = files.FindOne(ctx, bson.M{"fileDetails.data": bson.M{"$in": candidates}}).Decode(&record)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &record, nil
}

func (DBStore) SaveRecord(ctx context.Context, r *FileRecord) error {
	files, err := collection("files")
	if err != nil {
		return err
	}
	set := bson.M{"status": r.Status, "attachedTo": r.AttachedTo}
	if r.AttachedTo == nil {
		set["attachedTo"] = bson.A{}
	}
	if r.HoldReason != "" {
		set["holdReason"] = r.HoldReason
	}
	if !r.DeletedAt.IsZero() {
		set["deletedAt"] = r.DeletedAt
	}
	_, err = files.UpdateOne(ctx, bson.M{"_id": r.Ref}, bson.M{"$set": set})
	return err
}

func (s DBStore) IsReported(ctx context.Context, t Target) (bool, error) {
	pool, err := s.pool()
	if err != nil {
		return false, err
	}
	var found bool
	err = pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM entity_report WHERE target_type = $1 AND target_id = $2)`,
		t.Type, t.ID,
	).Scan(&found)
	return found, err
}

func (s DBStore) IsTargetLive(ctx context.Context, t Target) (bool, error) {
	var sql string
	switch t.Type {
	case "post":
		sql = `SELECT EXISTS (SELECT 1 FROM newsfeed_post WHERE post_id = $1 AND deleted_at IS NULL)`
	case "comment":
		sql = `SELECT EXISTS (SELECT 1 FROM newsfeed_comment WHERE comment_id = $1 AND deleted_at IS NULL)`
	case "message":
		messages, err := collection("messages")
		if err != nil {
			return false, err
		}
		n, err := messages.CountDocuments(ctx,
			bson.M{"messageID": t.ID, "isDeleted": bson.M{"$ne": true}},
		)
		return n > 0, err
	default:
		// Unknown kinds (a realm's avatar, ...) count as live: never delete on
		// a guess.
		return true, nil
	}
	pool, err := s.pool()
	if err != nil {
		return false, err
	}
	var live bool
	err = pool.QueryRow(ctx, sql, t.ID).Scan(&live)
	return live, err
}

var messageFolder = regexp.MustCompile(`^uploads/messages/([^/]+)/`)

// URLInUse is whether anything live still uses the URL, other than `except`.
// Checked by URL, so it covers files that predate upload records - and
// avatars, which are never recorded as attachments because a profile can
// change its picture without deleting anything.
func (s DBStore) URLInUse(ctx context.Context, rawURL, key string, except *Target, conversationID string) (bool, error) {
	pool, err := s.pool()
	if err != nil {
		return false, err
	}
	exceptPost, exceptComment := "", ""
	if except != nil && except.Type == "post" {
		exceptPost = except.ID
	}
	if except != nil && except.Type == "comment" {
		exceptComment = except.ID
	}
	var inUse bool
	err = pool.QueryRow(ctx, `SELECT
	   EXISTS (SELECT 1 FROM user_account WHERE profile = $1 OR coverphoto = $1)
	OR EXISTS (SELECT 1 FROM community_realm WHERE profile = $1 OR cover_photo = $1)
	OR EXISTS (
	     SELECT 1 FROM newsfeed_postreference r
	       JOIN newsfeed_post p ON p.post_id = r.post_id
	      WHERE (r.reference = $1 OR split_part(r.reference, '%%%', 1) = $1)
	        AND p.deleted_at IS NULL AND p.post_id <> $2)
	OR EXISTS (
	     SELECT 1 FROM newsfeed_post p
	      WHERE p.details -> 'poster' ->> 'url' = $1
	        AND p.deleted_at IS NULL AND p.post_id <> $2)
	OR EXISTS (
	     SELECT 1 FROM newsfeed_comment c
	      WHERE c.attachment = $1 AND c.deleted_at IS NULL AND c.comment_id <> $3)
	OR EXISTS (SELECT 1 FROM diary_attachment WHERE url = $1)`,
		rawURL, exceptPost, exceptComment,
	).Scan(&inUse)
	if err != nil || inUse {
		return inUse, err
	}

	if conversationID == "" {
		if m := messageFolder.FindStringSubmatch(key); m != nil {
			conversationID = m[1]
		}
	}
	if conversationID == "" {
		return false, nil
	}
	messages, err := collection("messages")
	if err != nil {
		return false, err
	}
	filter := bson.M{
		"conversationID": conversationID,
		"isDeleted":      bson.M{"$ne": true},
		"$or":            bson.A{bson.M{"content": rawURL}, bson.M{"attachment.url": rawURL}},
	}
	if except != nil && except.Type == "message" {
		filter["messageID"] = bson.M{"$ne": except.ID}
	}
	n, err := messages.CountDocuments(ctx, filter)
	return n > 0, err
}
