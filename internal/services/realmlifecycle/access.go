package realmlifecycle

import (
	"context"
	"errors"
	"sort"

	"github.com/jackc/pgx/v5"
)

// alias is a fixed code-owned SQL alias, never a request value. Root realms
// use their already-selected state; only a child needs the hierarchy subquery.
func ActiveRealmSQL(alias string) string {
	return `(` + alias + `.is_active AND ` + alias + `.deletion_requested_at IS NULL AND
	 CASE WHEN ` + alias + `.parent_id IS NULL THEN TRUE ELSE EXISTS(
       WITH RECURSIVE ancestry AS (
        SELECT id,parent_id,is_active,deletion_requested_at FROM community_realm WHERE id=` + alias + `.parent_id
        UNION SELECT p.id,p.parent_id,p.is_active,p.deletion_requested_at FROM ancestry a
        JOIN community_realm p ON p.id=a.parent_id WHERE a.parent_id IS NOT NULL
       ) SELECT COUNT(*) FROM ancestry HAVING COUNT(*)>0
       AND SUM(CASE WHEN is_active=TRUE AND deletion_requested_at IS NULL THEN 0 ELSE 1 END)=0
       AND SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END)>0
     ) END)`
}

var ErrScopeUnavailable = errors.New("realm scope unavailable")

// Used only while persisting an existing queued realm conversation notice.
// The caller keeps this read transaction open through its Mongo write so
// parent deactivation/deletion cannot pass an already admitted notice.
func LockActiveScopes(ctx context.Context, tx pgx.Tx, targetRealmID, actorEntityID string) error {
	rows, err := tx.Query(ctx, `WITH RECURSIVE ancestry AS (
	 SELECT id AS origin,id,parent_id,(is_active AND deletion_requested_at IS NULL) AS active
	 FROM community_realm WHERE id=$1 OR realm_id=$1 OR entity_id=$2
	 UNION SELECT chain.origin,parent.id,parent.parent_id,(parent.is_active AND parent.deletion_requested_at IS NULL)
	 FROM ancestry chain JOIN community_realm parent ON parent.id=chain.parent_id
	) SELECT origin,id,parent_id,active FROM ancestry`, targetRealmID, actorEntityID)
	if err != nil {
		return err
	}
	type node struct {
		id     string
		parent *string
		active bool
		root   string
		depth  int
	}
	nodes, origins := map[string]node{}, map[string]bool{}
	for rows.Next() {
		var origin string
		var n node
		if err := rows.Scan(&origin, &n.id, &n.parent, &n.active); err != nil {
			rows.Close()
			return err
		}
		nodes[n.id] = n
		origins[origin] = true
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if !origins[targetRealmID] {
		return ErrScopeUnavailable
	}
	held := map[string]node{}
	for origin := range origins {
		var path []node
		seen := map[string]bool{}
		id := origin
		for {
			n, ok := nodes[id]
			if !ok || id == "" || seen[id] || !n.active {
				return ErrScopeUnavailable
			}
			seen[id] = true
			path = append(path, n)
			if n.parent == nil {
				break
			}
			id = *n.parent
		}
		for index := len(path) - 1; index >= 0; index-- {
			n := path[index]
			n.root = path[len(path)-1].id
			n.depth = len(path) - 1 - index
			held[n.id] = n
		}
	}
	ordered := make([]node, 0, len(held))
	for _, n := range held {
		ordered = append(ordered, n)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.root != b.root {
			return a.root < b.root
		}
		if a.depth != b.depth {
			return a.depth < b.depth
		}
		return a.id < b.id
	})
	for _, expected := range ordered {
		var parent *string
		var active bool
		err := tx.QueryRow(ctx, `SELECT id,parent_id,(is_active AND deletion_requested_at IS NULL)
		 FROM community_realm WHERE id=$1 FOR SHARE`, expected.id).Scan(new(string), &parent, &active)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrScopeUnavailable
		}
		if err != nil {
			return err
		}
		if !active || (parent == nil) != (expected.parent == nil) || (parent != nil && *parent != *expected.parent) {
			return ErrScopeUnavailable
		}
	}
	return nil
}

func AuthorRealmAvailableSQL(entityExpression string) string {
	return `NOT EXISTS(SELECT 1 FROM community_realm lifecycle_author
	 WHERE lifecycle_author.entity_id=` + entityExpression + ` AND NOT ` + ActiveRealmSQL("lifecycle_author") + `)`
}

type accessQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func EffectiveActive(ctx context.Context, db accessQuerier, realmID string) (bool, error) {
	var allowed bool
	err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM community_realm r
	 WHERE (r.id=$1 OR r.realm_id=$1) AND `+ActiveRealmSQL("r")+`)`, realmID).Scan(&allowed)
	return allowed, err
}
