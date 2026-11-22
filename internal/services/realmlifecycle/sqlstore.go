package realmlifecycle

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

const nodeColumns = `id, realm_id, entity_id, COALESCE(parent_id, ''), type, is_private, is_active, deletion_requested_at,
 COALESCE((SELECT e.type FROM entity_entity e WHERE e.id=community_realm.entity_id),'')`

func scanNode(row pgx.Row) (*RealmNode, error) {
	var node RealmNode
	err := row.Scan(&node.ID, &node.RealmID, &node.EntityID, &node.ParentID,
		&node.Kind, &node.IsPrivate, &node.IsActive, &node.RequestedAt, &node.EntityKind)
	return &node, err
}

func (s SQLStore) LoadDeletionScope(ctx context.Context, job DeletionJob) (DeletionScope, error) {
	root, err := scanNode(s.DB.QueryRow(ctx, `SELECT `+nodeColumns+` FROM community_realm WHERE id=$1`, job.RealmID))
	if errors.Is(err, pgx.ErrNoRows) {
		return BuildDeletionScope(job, nil, nil, false)
	}
	if err != nil {
		return DeletionScope{}, err
	}
	if !matchingIntent(job, root) {
		return DeletionScope{}, ErrIntentMismatch
	}
	if root.ParentID != "" {
		var valid bool
		if err := s.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM community_realm WHERE id=$1 AND type='server')`, root.ParentID).Scan(&valid); err != nil {
			return DeletionScope{}, err
		}
		if !valid {
			return DeletionScope{}, ErrInvalidScope
		}
	}
	rows, err := s.DB.Query(ctx, `WITH RECURSIVE scope AS (
	 SELECT id FROM community_realm WHERE id=$1
	 UNION SELECT child.id FROM community_realm child JOIN scope parent ON child.parent_id=parent.id
	) SELECT `+nodeColumns+` FROM community_realm WHERE id IN (SELECT id FROM scope) ORDER BY id`, job.RealmID)
	if err != nil {
		return DeletionScope{}, err
	}
	var nodes []RealmNode
	for rows.Next() {
		Progress(ctx)
		node, err := scanNode(rows)
		if err != nil {
			rows.Close()
			return DeletionScope{}, err
		}
		nodes = append(nodes, *node)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return DeletionScope{}, err
	}
	var ownsOutside bool
	if root.Kind == "page" {
		err = s.DB.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM community_member m
		 JOIN entity_role r ON r.id=m.role_id AND r.realm_id=m.realm_id
		 WHERE m.entity_id=$1 AND m.realm_id<>$2 AND r.code='owner' AND r.is_default=TRUE)`,
			root.EntityID, root.ID).Scan(&ownsOutside)
		if err != nil {
			return DeletionScope{}, err
		}
	}
	return BuildDeletionScope(job, root, nodes, ownsOutside)
}

func (s SQLStore) PostBatch(ctx context.Context, scope DeletionScope, after string) ([]string, error) {
	if scope.NoOp {
		return nil, nil
	}
	rows, err := s.DB.Query(ctx, `SELECT p.post_id FROM newsfeed_post p
	 WHERE p.post_id > $1 AND (p.entity_id=ANY($2::text[]) OR EXISTS (
	 SELECT 1 FROM newsfeed_posttag t WHERE t.post_id=p.post_id AND t.entity_id=ANY($3::text[])))
	 ORDER BY p.post_id LIMIT $4`, after, scope.EntityIDs, scope.CommunityEntityIDs, BatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		Progress(ctx)
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if rows.Err() == nil {
		Progress(ctx)
	}
	return ids, rows.Err()
}
