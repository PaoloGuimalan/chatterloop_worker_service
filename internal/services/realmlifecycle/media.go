package realmlifecycle

import (
	"context"
	"fmt"
	"strings"

	"worker_service/internal/services/media"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const scopedPostSQL = `(p.entity_id=ANY($1::text[]) OR EXISTS (
 SELECT 1 FROM newsfeed_posttag t WHERE t.post_id=p.post_id AND t.entity_id=ANY($2::text[])))`

var sqlMediaSources = []string{
	`SELECT r.id || ':' || image.kind AS sort_key, 'realm_media' AS target_kind, r.id AS target_id, image.url
	 FROM community_realm r CROSS JOIN LATERAL (VALUES ('profile',r.profile),('cover',r.cover_photo)) image(kind,url)
	 WHERE r.id=ANY($3::text[])`,
	`SELECT ref.reference_id AS sort_key,'post' AS target_kind,p.post_id AS target_id,ref.reference AS url
	 FROM newsfeed_postreference ref JOIN newsfeed_post p ON p.post_id=ref.post_id WHERE ` + scopedPostSQL,
	`SELECT p.post_id AS sort_key,'post' AS target_kind,p.post_id AS target_id,p.details->'poster'->>'url' AS url
	 FROM newsfeed_post p WHERE ` + scopedPostSQL,
	`SELECT c.comment_id AS sort_key,'comment' AS target_kind,c.comment_id AS target_id,c.attachment AS url
	 FROM newsfeed_comment c WHERE c.entity_id=ANY($1::text[]) OR EXISTS (
	 SELECT 1 FROM newsfeed_post p WHERE p.post_id=c.post_id AND ` + scopedPostSQL + `)`,
	`SELECT a.id::text AS sort_key,'note' AS target_kind,n.id::text AS target_id,a.url
	 FROM notes_attachment a JOIN notes_entry n ON n.id=a.note_id WHERE n.entity_id=ANY($1::text[])`,
}

type mediaCandidate struct{ Key, Kind, ID, URL string }

func (s Store) releaseFile(ctx context.Context, scope DeletionScope, raw string, target *media.Target, conversationID string) error {
	if !strings.HasPrefix(raw, "http") {
		return nil
	}
	if s.Bucket == nil {
		return fmt.Errorf("%w: media storage", ErrDependency)
	}
	store := s.MediaStore
	if store == nil {
		store = media.DBStore{DeletingRealmIDs: scope.RealmIDs, DeletingEntityIDs: scope.EntityIDs}
	}
	_, err := media.ReleaseFile(ctx, store, s.Bucket, raw, target, conversationID, false)
	if err == nil {
		Progress(ctx)
	}
	return err
}

func (s Store) releaseSQLMedia(ctx context.Context, scope DeletionScope) error {
	for _, source := range sqlMediaSources {
		after := ""
		for {
			// Read a bounded batch and release its connection before media.DBStore
			// queries the same pool. Parallel workers cannot exhaust it with cursors.
			rows, err := s.DB.Query(ctx, `WITH scope_args AS (SELECT $1::text[] AS entities,
			 $2::text[] AS communities,$3::text[] AS realms)
			 SELECT sort_key,target_kind,target_id,url FROM (`+source+`) candidates
			 WHERE sort_key>$4 AND url IS NOT NULL AND url<>'' ORDER BY sort_key LIMIT $5`,
				scope.EntityIDs, scope.CommunityEntityIDs, scope.RealmIDs, after, BatchSize)
			if err != nil {
				return err
			}
			var batch []mediaCandidate
			for rows.Next() {
				Progress(ctx)
				var item mediaCandidate
				if err := rows.Scan(&item.Key, &item.Kind, &item.ID, &item.URL); err != nil {
					rows.Close()
					return err
				}
				batch = append(batch, item)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, item := range batch {
				if err := s.releaseFile(ctx, scope, item.URL, &media.Target{Type: item.Kind, ID: item.ID}, ""); err != nil {
					return err
				}
			}
			after = batch[len(batch)-1].Key
		}
	}
	return nil
}

func inIDs(ids []string) bson.M { return bson.M{"$in": append([]string{}, ids...)} }

func uploadScopeFilter(scope DeletionScope) bson.M {
	return bson.M{"status": bson.M{"$ne": "deleted"}, "$or": bson.A{
		bson.M{"ownerEntity": inIDs(scope.EntityIDs)}, bson.M{"context.realmID": inIDs(scope.RealmIDs)},
		bson.M{"context.conversationID": inIDs(scope.ConversationIDs)}, bson.M{"context.community_entity_id": inIDs(scope.CommunityEntityIDs)},
	}}
}

func (s Store) ReleaseMedia(ctx context.Context, scope DeletionScope) error {
	if s.Mongo == nil {
		return fmt.Errorf("%w: MongoDB", ErrDependency)
	}
	if err := s.releaseSQLMedia(ctx, scope); err != nil {
		return err
	}
	messages, err := s.Mongo.Collection("messages").Find(ctx, bson.M{"conversationID": inIDs(scope.ConversationIDs)},
		options.Find().SetBatchSize(BatchSize).SetProjection(bson.M{"messageID": 1, "conversationID": 1, "messageType": 1, "content": 1, "attachment.url": 1}))
	if err != nil {
		return err
	}
	defer messages.Close(ctx)
	for messages.Next(ctx) {
		Progress(ctx)
		var row struct {
			ID             string `bson:"messageID"`
			ConversationID string `bson:"conversationID"`
			Kind           string `bson:"messageType"`
			Content        any    `bson:"content"`
			Attachment     struct {
				URL string `bson:"url"`
			} `bson:"attachment"`
		}
		if err := messages.Decode(&row); err != nil {
			return err
		}
		target := &media.Target{Type: "message", ID: row.ID}
		if err := s.releaseFile(ctx, scope, row.Attachment.URL, target, row.ConversationID); err != nil {
			return err
		}
		if row.Kind != "text" && row.Kind != "notif" && row.Kind != "post" {
			if raw, ok := row.Content.(string); ok {
				if err := s.releaseFile(ctx, scope, raw, target, row.ConversationID); err != nil {
					return err
				}
			}
		}
	}
	if err := messages.Err(); err != nil {
		return err
	}
	files, err := s.Mongo.Collection("files").Find(ctx, uploadScopeFilter(scope), options.Find().SetBatchSize(BatchSize).SetProjection(bson.M{"fileDetails.data": 1}))
	if err != nil {
		return err
	}
	defer files.Close(ctx)
	for files.Next(ctx) {
		Progress(ctx)
		var row struct {
			Details struct {
				URL string `bson:"data"`
			} `bson:"fileDetails"`
		}
		if err := files.Decode(&row); err != nil {
			return err
		}
		if err := s.releaseFile(ctx, scope, row.Details.URL, nil, ""); err != nil {
			return err
		}
	}
	return files.Err()
}
