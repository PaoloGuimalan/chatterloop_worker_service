package realmlifecycle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

type recordedScopeTx struct {
	pgx.Tx
	locks []string
}

func (s *recordedScopeTx) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if strings.Contains(query, "FOR SHARE") {
		s.locks = append(s.locks, args[0].(string))
	}
	return s.Tx.QueryRow(ctx, query, args...)
}

func TestScopeLocksPostgresKeepParentFirstAndRejectMissingOrInactiveScope(t *testing.T) {
	db := scopeDB(t)
	ctx := t.Context()
	_, err := db.Exec(ctx, `INSERT INTO entity_entity(id) VALUES ('server-e'),('child-e');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,is_active) VALUES ('z-server','z-server','server-e','server',TRUE);
	 INSERT INTO community_realm(id,realm_id,entity_id,type,is_active,parent_id) VALUES ('a-child','a-child','child-e','channel',TRUE,'z-server')`)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	recorded := &recordedScopeTx{Tx: tx}
	if err := LockActiveScopes(ctx, recorded, "a-child", "personal-actor"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(recorded.locks, []string{"z-server", "a-child"}) {
		t.Fatalf("wrong lock order: %v", recorded.locks)
	}
	if err := LockActiveScopes(ctx, recorded, "missing", "personal-actor"); !errors.Is(err, ErrScopeUnavailable) {
		t.Fatalf("missing realm accepted: %v", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE community_realm SET is_active=FALSE WHERE id='z-server'`); err != nil {
		t.Fatal(err)
	}
	if err := LockActiveScopes(ctx, recorded, "a-child", "personal-actor"); !errors.Is(err, ErrScopeUnavailable) {
		t.Fatalf("inactive ancestor accepted: %v", err)
	}
}
