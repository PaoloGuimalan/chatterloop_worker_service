package realmlifecycle

import (
	"context"
	"errors"
)

var ErrUnpreparedContent = errors.New("realm content is not prepared for cleanup")

// Keep the existing report evidence, hidden by deleted_at. A report on a
// comment also retains its parent post so the moderator keeps its context.
const postReportedSQL = `EXISTS (SELECT 1 FROM entity_report report WHERE
 (report.target_type='post' AND report.target_id=p.post_id) OR
 (report.target_type='comment' AND report.target_id IN (
  SELECT c.comment_id FROM newsfeed_comment c WHERE c.post_id=p.post_id)))`

func (s SQLStore) purgePostBatch(ctx context.Context, ids []string) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT p.post_id,p.deleted_at IS NOT NULL,`+postReportedSQL+`
	 FROM newsfeed_post p WHERE p.post_id=ANY($1::text[]) ORDER BY p.post_id FOR UPDATE OF p`, ids)
	if err != nil {
		return err
	}
	var removable []string
	for rows.Next() {
		Progress(ctx)
		var id string
		var prepared, reported bool
		if err := rows.Scan(&id, &prepared, &reported); err != nil {
			rows.Close()
			return err
		}
		if !prepared {
			rows.Close()
			return ErrUnpreparedContent
		}
		if !reported {
			removable = append(removable, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(removable) > 0 {
		for _, statement := range []string{
			`DELETE FROM newsfeed_commentreaction WHERE comment_id IN (SELECT comment_id FROM newsfeed_comment WHERE post_id=ANY($1::text[]))`,
			`DELETE FROM newsfeed_commentpreviewcount WHERE comment_id IN (SELECT comment_id FROM newsfeed_comment WHERE post_id=ANY($1::text[]))`,
			`DELETE FROM newsfeed_comment WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_mapview WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_reaction WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_previewcount WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_activitycount WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_postscore WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_postsave WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_postprivacy WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_posttag WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_postreference WHERE post_id=ANY($1::text[])`,
			`DELETE FROM interests_postinterestlink WHERE post_id=ANY($1::text[])`,
			`DELETE FROM newsfeed_post WHERE post_id=ANY($1::text[])`,
		} {
			if _, err := tx.Exec(ctx, statement, removable); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s SQLStore) PurgePosts(ctx context.Context, scope DeletionScope) error {
	after := ""
	for {
		ids, err := s.PostBatch(ctx, scope, after)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if err := s.purgePostBatch(ctx, ids); err != nil {
			return err
		}
		after = ids[len(ids)-1]
	}
	// Surviving replies keep the existing deleted-parent placeholder. Clear
	// only the deleted realm's body/media, after media release has completed.
	return s.clearActorComments(ctx, scope)
}

func (s SQLStore) clearActorComments(ctx context.Context, scope DeletionScope) error {
	return s.repeatCounted(ctx, `WITH cleared AS (UPDATE newsfeed_comment SET text='',attachment=NULL
	 WHERE comment_id IN (SELECT c.comment_id FROM newsfeed_comment c
	 WHERE c.entity_id=ANY($1::text[]) AND c.deleted_at IS NOT NULL
	 AND (c.text IS DISTINCT FROM '' OR c.attachment IS NOT NULL)
	 AND NOT EXISTS (SELECT 1 FROM entity_report r WHERE r.target_type='comment' AND r.target_id=c.comment_id)
	 ORDER BY c.comment_id LIMIT 100) RETURNING comment_id) SELECT COUNT(*) FROM cleared`, scope.EntityIDs)
}
