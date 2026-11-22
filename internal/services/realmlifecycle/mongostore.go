package realmlifecycle

import (
	"context"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Receipts point to notificationID, not to the realm or Mongo _id. Remove
// receipts before their source rows so a retry can always find what remains.
func (s Store) deleteNotices(ctx context.Context, filter bson.M) error {
	if s.Mongo == nil {
		return fmt.Errorf("%w: MongoDB", ErrDependency)
	}
	collection := s.Mongo.Collection("notifications")
	rows, err := collection.Find(ctx, filter, options.Find().SetBatchSize(BatchSize).SetSort(bson.D{{Key: "_id", Value: 1}}).SetProjection(bson.M{"notificationID": 1}))
	if err != nil {
		return err
	}
	defer rows.Close(ctx)
	var ids, references []any
	flush := func() error {
		if len(ids) == 0 {
			return nil
		}
		if len(references) > 0 {
			if _, err := collection.DeleteMany(ctx, bson.M{"type": "community_admin_inbox", "referenceID": bson.M{"$in": references}}); err != nil {
				return err
			}
		}
		_, err := collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
		ids, references = nil, nil
		if err == nil {
			Progress(ctx)
		}
		return err
	}
	for rows.Next(ctx) {
		Progress(ctx)
		var row struct {
			ID        any `bson:"_id"`
			Reference any `bson:"notificationID"`
		}
		if err := rows.Decode(&row); err != nil {
			return err
		}
		ids = append(ids, row.ID)
		if row.Reference != nil {
			references = append(references, row.Reference)
		}
		if len(ids) == BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return flush()
}

func (s Store) purgeOwnedNotifications(ctx context.Context, scope DeletionScope) error {
	if err := s.deleteNotices(ctx, bson.M{"toUserID": inIDs(scope.EntityIDs)}); err != nil {
		return err
	}
	return s.deleteNotices(ctx, bson.M{"type": "community_admin_activity",
		"fromUserID": inIDs(scope.CommunityEntityIDs), "referenceID": inIDs(scope.CommunityEntityIDs)})
}

type noticeReferences struct {
	query string
	kinds []string
}

func (s Store) purgeReferenceBatches(ctx context.Context, source noticeReferences, ids []string) error {
	after := ""
	for {
		rows, err := s.DB.Query(ctx, source.query, ids, after, BatchSize)
		if err != nil {
			return err
		}
		var references []string
		for rows.Next() {
			Progress(ctx)
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			references = append(references, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(references) == 0 {
			return nil
		}
		if err := s.deleteNotices(ctx, bson.M{"type": inIDs(source.kinds), "referenceID": inIDs(references)}); err != nil {
			return err
		}
		after = references[len(references)-1]
	}
}

func (s Store) purgeActorActivityNotices(ctx context.Context, scope DeletionScope) error {
	for _, source := range []noticeReferences{
		{`SELECT comment_id FROM newsfeed_comment WHERE entity_id=ANY($1::text[]) AND comment_id>$2 ORDER BY comment_id LIMIT $3`, []string{"post_comment", "comment_reply", "comment_mention"}},
		{`SELECT reaction_id FROM newsfeed_reaction WHERE entity_id=ANY($1::text[]) AND reaction_id>$2 ORDER BY reaction_id LIMIT $3`, []string{"post_reaction"}},
		{`SELECT reaction_id FROM newsfeed_commentreaction WHERE entity_id=ANY($1::text[]) AND reaction_id>$2 ORDER BY reaction_id LIMIT $3`, []string{"comment_reaction"}},
	} {
		if err := s.purgeReferenceBatches(ctx, source, scope.EntityIDs); err != nil {
			return err
		}
	}
	return nil
}

func (s Store) purgePostNotifications(ctx context.Context, ids []string) error {
	for _, source := range []noticeReferences{
		{`SELECT comment_id FROM newsfeed_comment WHERE post_id=ANY($1::text[]) AND comment_id>$2 ORDER BY comment_id LIMIT $3`, []string{"post_comment", "comment_reply", "comment_mention"}},
		{`SELECT reaction_id FROM newsfeed_reaction WHERE post_id=ANY($1::text[]) AND reaction_id>$2 ORDER BY reaction_id LIMIT $3`, []string{"post_reaction"}},
		{`SELECT r.reaction_id FROM newsfeed_commentreaction r JOIN newsfeed_comment c ON c.comment_id=r.comment_id WHERE c.post_id=ANY($1::text[]) AND r.reaction_id>$2 ORDER BY r.reaction_id LIMIT $3`, []string{"comment_reaction"}},
	} {
		if err := s.purgeReferenceBatches(ctx, source, ids); err != nil {
			return err
		}
	}
	return s.deleteNotices(ctx, bson.M{"$or": bson.A{
		bson.M{"target.type": "post", "target.supportingID": inIDs(ids)},
		bson.M{"type": inIDs([]string{"tag_notification", "shared_post_notification", "community_post_created"}), "referenceID": inIDs(ids)},
	}})
}

func (s Store) PurgeContent(ctx context.Context, scope DeletionScope) error {
	if s.Mongo == nil {
		return fmt.Errorf("%w: MongoDB", ErrDependency)
	}
	if err := s.purgeOwnedNotifications(ctx, scope); err != nil {
		return err
	}
	if err := s.purgeRealmNotices(ctx, scope); err != nil {
		return err
	}
	after := ""
	for {
		ids, err := s.PostBatch(ctx, scope, after)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if err := s.purgePostNotifications(ctx, ids); err != nil {
			return err
		}
		if err := s.purgePostBatch(ctx, ids); err != nil {
			return err
		}
		after = ids[len(ids)-1]
	}
	if err := s.clearActorComments(ctx, scope); err != nil {
		return err
	}
	if err := s.purgeConversationMessages(ctx, scope); err != nil {
		return err
	}
	for _, name := range []string{"conversations"} {
		if _, err := s.Mongo.Collection(name).DeleteMany(ctx, bson.M{"conversationID": inIDs(scope.ConversationIDs)}); err != nil {
			return err
		}
	}
	_, err := s.Mongo.Collection("chat_history").DeleteMany(ctx, bson.M{"$or": bson.A{
		bson.M{"conversationID": inIDs(scope.ConversationIDs)}, bson.M{"entityID": inIDs(scope.EntityIDs)},
	}})
	return err
}

// Reports refer to messageID; their text is evidence just like an ordinary
// unsent message. Keep only those soft-deleted documents, without conversation
// membership, previews or history through which app clients could read them.
func (s Store) purgeConversationMessages(ctx context.Context, scope DeletionScope) error {
	cur, err := s.Mongo.Collection("messages").Find(ctx, bson.M{"conversationID": inIDs(scope.ConversationIDs)}, options.Find().SetBatchSize(BatchSize).SetProjection(bson.M{"messageID": 1}))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	var batch []bson.M
	flush := func() error {
		var ids []string
		for _, doc := range batch {
			if id, ok := doc["messageID"].(string); ok {
				ids = append(ids, id)
			}
		}
		rows, err := s.DB.Query(ctx, `SELECT target_id FROM entity_report WHERE target_type='message' AND target_id=ANY($1::text[])`, ids)
		if err != nil {
			return err
		}
		reported := map[string]bool{}
		for rows.Next() {
			Progress(ctx)
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			reported[id] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		var remove []any
		for _, doc := range batch {
			id, _ := doc["messageID"].(string)
			if !reported[id] {
				remove = append(remove, doc["_id"])
			}
		}
		if len(remove) > 0 {
			_, err = s.Mongo.Collection("messages").DeleteMany(ctx, bson.M{"_id": bson.M{"$in": remove}})
		}
		batch = nil
		return err
	}
	for cur.Next(ctx) {
		Progress(ctx)
		var doc bson.M
		if err := cur.Decode(&doc); err != nil {
			return err
		}
		batch = append(batch, doc)
		if len(batch) == BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	if len(batch) > 0 {
		return flush()
	}
	return nil
}

func (s Store) purgeRealmNotices(ctx context.Context, scope DeletionScope) error {
	after := ""
	for {
		rows, err := s.DB.Query(ctx, `SELECT id::text FROM community_invite
		 WHERE id::text>$3 AND (realm_id=ANY($1::text[]) OR target_entity_id=ANY($2::text[])
		 OR (created_by_id=ANY($2::text[]) AND status IN ('pending','for_approval')))
		 ORDER BY id::text LIMIT $4`, scope.RealmIDs, scope.EntityIDs, after, BatchSize)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			Progress(ctx)
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if err := s.deleteNotices(ctx, bson.M{"type": inIDs([]string{"realm_invite", "community_join_request"}), "referenceID": inIDs(ids)}); err != nil {
			return err
		}
		after = ids[len(ids)-1]
	}
}

func (s Store) Finalize(ctx context.Context, scope DeletionScope) error {
	if s.Mongo == nil {
		return fmt.Errorf("%w: MongoDB", ErrDependency)
	}
	known, retained := map[string]bool{}, map[string]bool{}
	for _, id := range scope.EntityIDs {
		known[id] = true
	}
	for _, source := range []struct {
		collection, field string
		filter            bson.M
	}{
		{"messages", "sender", bson.M{"sender": inIDs(scope.EntityIDs)}},
		{"messages", "reactions.entityID", bson.M{"reactions.entityID": inIDs(scope.EntityIDs)}},
		{"conversations", "participant_ids", bson.M{"participant_ids": inIDs(scope.EntityIDs)}},
		{"notifications", "fromUserID", bson.M{"fromUserID": inIDs(scope.EntityIDs)}},
		{"files", "ownerEntity", bson.M{"ownerEntity": inIDs(scope.EntityIDs), "status": bson.M{"$ne": "deleted"}}},
	} {
		ids, err := s.Mongo.Collection(source.collection).Distinct(ctx, source.field, source.filter)
		if err != nil {
			return err
		}
		for _, value := range ids {
			if id, ok := value.(string); ok && known[id] {
				retained[id] = true
			}
		}
	}
	scope.RetainEntityIDs = nil
	for id := range retained {
		scope.RetainEntityIDs = append(scope.RetainEntityIDs, id)
	}
	sort.Strings(scope.RetainEntityIDs)
	return s.SQLStore.Finalize(ctx, scope)
}
