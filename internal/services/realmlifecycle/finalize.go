package realmlifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
)

var _ DeletionStore = Store{}

func (s SQLStore) Finalize(ctx context.Context, scope DeletionScope) error {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id string
	err = tx.QueryRow(ctx, `SELECT id FROM community_realm WHERE id=$1 FOR UPDATE`, scope.Job.RealmID).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return tx.Commit(ctx)
	}
	if err != nil {
		return err
	}
	locked := SQLStore{DB: tx}
	current, err := locked.LoadDeletionScope(ctx, scope.Job)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(scope.RealmIDs, current.RealmIDs) || !reflect.DeepEqual(scope.EntityIDs, current.EntityIDs) {
		return ErrInvalidScope
	}
	if err := locked.CheckInventory(ctx); err != nil {
		return err
	}
	for _, node := range current.Nodes {
		if node.ID != scope.Job.RealmID {
			if err := tx.QueryRow(ctx, `SELECT id FROM community_realm WHERE id=$1 FOR UPDATE`, node.ID).Scan(&id); err != nil {
				return err
			}
		}
	}
	var remaining bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM newsfeed_post p WHERE `+scopedPostSQL+`
	 AND (p.deleted_at IS NULL OR NOT (`+postReportedSQL+`)))`, current.EntityIDs, current.CommunityEntityIDs).Scan(&remaining)
	if err != nil {
		return err
	}
	if remaining {
		return ErrUnpreparedContent
	}
	steps := []struct {
		query string
		args  []any
	}{
		{`DELETE FROM community_approvals WHERE realm_id=ANY($1::text[])`, []any{current.RealmIDs}},
		{`DELETE FROM community_invite WHERE realm_id=ANY($1::text[])`, []any{current.RealmIDs}},
		{`UPDATE community_invite SET status='revoked',resolved_at=NOW()
		 WHERE created_by_id=ANY($1::text[]) AND status IN ('pending','for_approval')`, []any{current.EntityIDs}},
		{`UPDATE community_invite SET status=CASE WHEN status IN ('pending','for_approval') THEN 'revoked' ELSE status END,
		 resolved_at=CASE WHEN status IN ('pending','for_approval') THEN NOW() ELSE resolved_at END,target_entity_id=NULL
		 WHERE target_entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM community_member WHERE realm_id=ANY($1::text[]) OR entity_id=ANY($2::text[])`, []any{current.RealmIDs, current.EntityIDs}},
		{`DELETE FROM entity_permission WHERE realm_id=ANY($1::text[]) OR entity_id=ANY($2::text[])`, []any{current.RealmIDs, current.EntityIDs}},
		{`UPDATE entity_permission SET created_by_id=NULL WHERE created_by_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_token WHERE realm_id=ANY($1::text[]) OR entity_id=ANY($2::text[])`, []any{current.RealmIDs, current.EntityIDs}},
		{`UPDATE entity_token SET created_by_id=NULL WHERE created_by_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_role_permissions WHERE role_id IN (SELECT id FROM entity_role WHERE realm_id=ANY($1::text[]))`, []any{current.RealmIDs}},
		{`DELETE FROM entity_role WHERE realm_id=ANY($1::text[])`, []any{current.RealmIDs}},
		{`DELETE FROM notes_attachment WHERE note_id IN (SELECT id FROM notes_entry WHERE entity_id=ANY($1::text[]))`, []any{current.EntityIDs}},
		{`DELETE FROM notes_entry WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_encryption_configuration_purposes WHERE encryptionconfiguration_id IN
		 (SELECT id FROM entity_encryption_configuration WHERE entity_id=ANY($1::text[]))`, []any{current.EntityIDs}},
		{`UPDATE entity_encryption_configuration SET replaces_id=NULL WHERE replaces_id IN
		 (SELECT id FROM entity_encryption_configuration WHERE entity_id=ANY($1::text[]))`, []any{current.EntityIDs}},
		{`DELETE FROM entity_encryption_configuration WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_connection WHERE action_by_id=ANY($1::text[]) OR involved_entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_follow WHERE follower_id=ANY($1::text[]) OR followee_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM entity_block WHERE blocker_id=ANY($1::text[]) OR blocked_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM interests_entityinterestaffinity WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM interests_entityinterest WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`UPDATE interests_entityinterest SET created_by_id=NULL WHERE created_by_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM newsfeed_postsave WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM newsfeed_postprivacy WHERE allowed_entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
		{`DELETE FROM newsfeed_posttag WHERE entity_id=ANY($1::text[])`, []any{current.EntityIDs}},
	}
	for _, step := range steps {
		if _, err := tx.Exec(ctx, step.query, step.args...); err != nil {
			return err
		}
	}
	for _, node := range current.Nodes {
		if _, err := tx.Exec(ctx, `DELETE FROM community_realm WHERE id=$1`, node.ID); err != nil {
			return err
		}
	}
	if err := locked.removeUnreferencedEntities(ctx, current.EntityIDs, scope.RetainEntityIDs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s SQLStore) removeUnreferencedEntities(ctx context.Context, ids, retained []string) error {
	var manifest []schemaReference
	if err := json.Unmarshal(inventoryJSON, &manifest); err != nil {
		return err
	}
	var predicates []string
	for _, ref := range manifest {
		if ref.Target == "entity_entity" {
			predicates = append(predicates, `NOT EXISTS(SELECT 1 FROM `+pgx.Identifier{ref.Table}.Sanitize()+` history WHERE history.`+pgx.Identifier{ref.Column}.Sanitize()+`=e.id)`)
		}
	}
	if len(predicates) == 0 {
		return ErrUnknownReference
	}
	_, err := s.DB.Exec(ctx, `DELETE FROM entity_entity e WHERE e.id=ANY($1::text[])
	 AND e.id<>ALL(COALESCE($2::text[],'{}')) AND e.type='realm' AND `+strings.Join(predicates, " AND "), ids, retained)
	return err
}
