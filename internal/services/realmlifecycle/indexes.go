package realmlifecycle

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type cqlRows interface {
	Scan(...interface{}) bool
	Close() error
}
type indexStore interface {
	Iter(context.Context, string, ...any) cqlRows
	DeleteBatch(context.Context, string, [][]any) error
}
type cassandraIndexes struct{ session *gocql.Session }

func (s cassandraIndexes) Iter(ctx context.Context, query string, args ...any) cqlRows {
	return s.session.Query(query, args...).WithContext(ctx).PageSize(BatchSize).Iter()
}
func (s cassandraIndexes) DeleteBatch(ctx context.Context, query string, keys [][]any) error {
	batch := s.session.NewBatch(gocql.UnloggedBatch).WithContext(ctx)
	for _, key := range keys {
		batch.Query(query, key...)
	}
	return s.session.ExecuteBatch(batch)
}

func deleteIndexRows(ctx context.Context, store indexStore, rows cqlRows, statement string, read func(cqlRows) ([]any, bool)) error {
	closed := false
	defer func() {
		if !closed {
			_ = rows.Close()
		}
	}()
	var batch [][]any
	for {
		key, ok := read(rows)
		if !ok {
			break
		}
		batch = append(batch, key)
		if len(batch) == BatchSize {
			if err := store.DeleteBatch(ctx, statement, batch); err != nil {
				return err
			}
			batch = nil
			Progress(ctx)
		}
	}
	err := rows.Close()
	closed = true
	if err != nil {
		return err
	}
	Progress(ctx)
	if len(batch) > 0 {
		return store.DeleteBatch(ctx, statement, batch)
	}
	return nil
}

func deleteEngagementTarget(ctx context.Context, store indexStore, id string) error {
	return deleteIndexRows(ctx, store, store.Iter(ctx, `SELECT user_id,log_id,activity_time FROM user_engagement_log
	 WHERE target_id=? ALLOW FILTERING`, id), `DELETE FROM user_engagement_log WHERE user_id=? AND log_id=? AND activity_time=?`,
		func(rows cqlRows) ([]any, bool) {
			var user, log gocql.UUID
			var at time.Time
			ok := rows.Scan(&user, &log, &at)
			return []any{user, log, at}, ok
		})
}

func deletePostIndexes(ctx context.Context, store indexStore, id string) error {
	for _, table := range []struct{ selectSQL, deleteSQL string }{
		{`SELECT bucket,post_id,created_at FROM newsfeed_index WHERE post_id=? ALLOW FILTERING`, `DELETE FROM newsfeed_index WHERE bucket=? AND post_id=? AND created_at=?`},
		{`SELECT category,post_id,created_at FROM trending_pool WHERE post_id=? ALLOW FILTERING`, `DELETE FROM trending_pool WHERE category=? AND post_id=? AND created_at=?`},
	} {
		if err := deleteIndexRows(ctx, store, store.Iter(ctx, table.selectSQL, id), table.deleteSQL,
			func(rows cqlRows) ([]any, bool) {
				var bucket, post string
				var at time.Time
				ok := rows.Scan(&bucket, &post, &at)
				return []any{bucket, post, at}, ok
			}); err != nil {
			return err
		}
	}
	return deleteEngagementTarget(ctx, store, id)
}

func (s Store) RemoveIndexes(ctx context.Context, scope DeletionScope) error {
	if s.Cassandra == nil && s.indexes == nil {
		return fmt.Errorf("%w: Cassandra", ErrDependency)
	}
	var index indexStore = s.indexes
	if index == nil {
		index = cassandraIndexes{s.Cassandra}
	}
	for _, entityID := range scope.EntityIDs {
		if err := index.DeleteBatch(ctx, `DELETE FROM newsfeed_index WHERE bucket=?`, [][]any{{entityID}}); err != nil {
			return err
		}
		// Engagement partitions are UUIDs. Legacy non-UUID entities could never
		// write this partition through the existing logger in the first place.
		if id, err := gocql.ParseUUID(entityID); err == nil {
			if err := index.DeleteBatch(ctx, `DELETE FROM user_engagement_log WHERE user_id=?`, [][]any{{id}}); err != nil {
				return err
			}
		}
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
		for _, id := range ids {
			if err := deletePostIndexes(ctx, index, id); err != nil {
				return err
			}
		}
		after = ids[len(ids)-1]
	}
	for _, query := range []string{
		`SELECT c.comment_id FROM newsfeed_comment c WHERE c.comment_id>$3 AND (c.entity_id=ANY($1::text[]) OR EXISTS
		 (SELECT 1 FROM newsfeed_post p WHERE p.post_id=c.post_id AND ` + scopedPostSQL + `)) ORDER BY c.comment_id LIMIT $4`,
		`SELECT r.reaction_id FROM newsfeed_reaction r JOIN newsfeed_post p ON p.post_id=r.post_id
		 WHERE r.reaction_id>$3 AND ` + scopedPostSQL + ` ORDER BY r.reaction_id LIMIT $4`,
		`SELECT r.reaction_id FROM newsfeed_commentreaction r JOIN newsfeed_comment c ON c.comment_id=r.comment_id
		 WHERE r.reaction_id>$3 AND (c.entity_id=ANY($1::text[]) OR EXISTS (SELECT 1 FROM newsfeed_post p
		 WHERE p.post_id=c.post_id AND ` + scopedPostSQL + `)) ORDER BY r.reaction_id LIMIT $4`,
	} {
		after = ""
		for {
			rows, err := s.DB.Query(ctx, query, scope.EntityIDs, scope.CommunityEntityIDs, after, BatchSize)
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
				break
			}
			for _, id := range ids {
				if err := deleteEngagementTarget(ctx, index, id); err != nil {
					return err
				}
			}
			after = ids[len(ids)-1]
		}
	}
	return nil
}
