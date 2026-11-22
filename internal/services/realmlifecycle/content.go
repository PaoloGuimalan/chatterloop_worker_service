package realmlifecycle

import "context"

// Count changes come from the rows actually changed in this transaction, so
// retries and concurrent removals cannot apply a second decrement.
const markCommentsSQL = `WITH marked AS (
 UPDATE newsfeed_comment SET deleted_at=NOW() WHERE comment_id IN (
  SELECT comment_id FROM newsfeed_comment WHERE deleted_at IS NULL
  AND (entity_id=ANY($1::text[]) OR post_id=ANY($2::text[])) ORDER BY comment_id LIMIT 100
 ) RETURNING post_id
), totals AS (SELECT post_id, COUNT(*)::int AS n FROM marked GROUP BY post_id),
scores AS (UPDATE newsfeed_postscore s SET comments_count=GREATEST(0,s.comments_count-t.n)
 FROM totals t WHERE s.post_id=t.post_id RETURNING s.post_id),
activities AS (UPDATE newsfeed_activitycount a SET count=GREATEST(0,a.count-t.n)
 FROM totals t WHERE a.post_id=t.post_id AND a.count_type='comment' RETURNING a.post_id)
SELECT COUNT(*) FROM marked`

const removeReactionsSQL = `WITH removed AS (
 DELETE FROM newsfeed_reaction WHERE reaction_id IN (
  SELECT reaction_id FROM newsfeed_reaction WHERE entity_id=ANY($1::text[]) ORDER BY reaction_id LIMIT 100
 ) RETURNING post_id, emoji_id
), totals AS (SELECT post_id, COUNT(*)::int AS n FROM removed GROUP BY post_id),
emojis AS (SELECT post_id,emoji_id,COUNT(*)::int AS n FROM removed GROUP BY post_id,emoji_id),
scores AS (UPDATE newsfeed_postscore s SET likes_count=GREATEST(0,s.likes_count-t.n)
 FROM totals t WHERE s.post_id=t.post_id RETURNING s.post_id),
previews AS (UPDATE newsfeed_previewcount p SET count=GREATEST(0,p.count-e.n)
 FROM emojis e WHERE p.post_id=e.post_id AND p.emoji_id IS NOT DISTINCT FROM e.emoji_id RETURNING p.post_id)
SELECT COUNT(*) FROM removed`

const removeCommentReactionsSQL = `WITH removed AS (
 DELETE FROM newsfeed_commentreaction WHERE reaction_id IN (
  SELECT reaction_id FROM newsfeed_commentreaction WHERE entity_id=ANY($1::text[]) ORDER BY reaction_id LIMIT 100
 ) RETURNING comment_id,emoji_id
), emojis AS (SELECT comment_id,emoji_id,COUNT(*)::int AS n FROM removed GROUP BY comment_id,emoji_id),
previews AS (UPDATE newsfeed_commentpreviewcount p SET count=GREATEST(0,p.count-e.n)
 FROM emojis e WHERE p.comment_id=e.comment_id AND p.emoji_id IS NOT DISTINCT FROM e.emoji_id RETURNING p.comment_id)
SELECT COUNT(*) FROM removed`

func (s SQLStore) repeatCounted(ctx context.Context, sql string, args ...any) error {
	for {
		var changed int
		if err := s.DB.QueryRow(ctx, sql, args...).Scan(&changed); err != nil {
			return err
		}
		if changed < BatchSize {
			return nil
		}
		Progress(ctx)
	}
}

// Create/lock the score before removing its activity. This also serializes
// cleanup with a queued increment that is creating the score for the first time.
func (s SQLStore) repeatCountedWithScores(ctx context.Context, sql, targets string, args ...any) error {
	for {
		tx, err := s.DB.Begin(ctx)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO newsfeed_postscore
		 (post_id,affinity_score,content_type_weight,recent_update_boost,likes_count,comments_count,shares_count,ranking_score)
		 SELECT DISTINCT post_id,1.,1.,1.,0,0,0,0. FROM (`+targets+`) targets ORDER BY post_id
		 ON CONFLICT(post_id) DO NOTHING`, args...)
		var changed int
		if err == nil {
			err = tx.QueryRow(ctx, sql, args...).Scan(&changed)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err = tx.Commit(ctx); err != nil {
			return err
		}
		Progress(ctx)
		if changed < BatchSize {
			return nil
		}
	}
}

const commentScoreTargets = `SELECT post_id FROM newsfeed_comment WHERE deleted_at IS NULL
 AND (entity_id=ANY($1::text[]) OR post_id=ANY($2::text[])) ORDER BY comment_id LIMIT 100`
const reactionScoreTargets = `SELECT post_id FROM newsfeed_reaction
 WHERE entity_id=ANY($1::text[]) ORDER BY reaction_id LIMIT 100`

func (s SQLStore) PrepareContent(ctx context.Context, scope DeletionScope) error {
	if err := s.repeatCountedWithScores(ctx, markCommentsSQL, commentScoreTargets, scope.EntityIDs, []string{}); err != nil {
		return err
	}
	if err := s.repeatCountedWithScores(ctx, removeReactionsSQL, reactionScoreTargets, scope.EntityIDs); err != nil {
		return err
	}
	if err := s.repeatCounted(ctx, removeCommentReactionsSQL, scope.EntityIDs); err != nil {
		return err
	}
	after := ""
	for {
		ids, err := s.PostBatch(ctx, scope, after)
		if err != nil || len(ids) == 0 {
			return err
		}
		if _, err := s.DB.Exec(ctx, `UPDATE newsfeed_post SET deleted_at=NOW()
		 WHERE post_id=ANY($1::text[]) AND deleted_at IS NULL`, ids); err != nil {
			return err
		}
		if err := s.repeatCountedWithScores(ctx, markCommentsSQL, commentScoreTargets, []string{}, ids); err != nil {
			return err
		}
		after = ids[len(ids)-1]
	}
}
