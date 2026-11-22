package realmlifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// Direct disposable DDL only, with the actual classified foreign keys. This
// exercises dependency ordering without invoking an application migration.
func completeInventoryFixture(t *testing.T, db *pgx.Conn) {
	t.Helper()
	var refs []schemaReference
	if err := json.Unmarshal(inventoryJSON, &refs); err != nil {
		t.Fatal(err)
	}
	tables := map[string]bool{}
	for _, ref := range refs {
		tables[ref.Table] = true
		tables[ref.Target] = true
	}
	var names []string
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		key := "id"
		if name == "newsfeed_comment" {
			key = "comment_id"
		}
		if _, err := db.Exec(t.Context(), `CREATE TABLE IF NOT EXISTS `+pgx.Identifier{name}.Sanitize()+` (`+pgx.Identifier{key}.Sanitize()+` text PRIMARY KEY)`); err != nil {
			t.Fatal(err)
		}
	}
	for index, ref := range refs {
		kind := "text"
		if ref.Target == "entity_role" {
			kind = "bigint"
		}
		if _, err := db.Exec(t.Context(), `ALTER TABLE `+pgx.Identifier{ref.Table}.Sanitize()+` ADD COLUMN IF NOT EXISTS `+pgx.Identifier{ref.Column}.Sanitize()+` `+kind); err != nil {
			t.Fatal(err)
		}
		var exists bool
		err := db.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM pg_constraint c JOIN pg_attribute a
		 ON a.attrelid=c.conrelid AND a.attnum=ANY(c.conkey) WHERE c.contype='f' AND c.conrelid=$1::regclass AND a.attname=$2)`, ref.Table, ref.Column).Scan(&exists)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			constraint := pgx.Identifier{fmt.Sprintf("%s_fixture_%d", ref.Table, index)}.Sanitize()
			_, err = db.Exec(t.Context(), `ALTER TABLE `+pgx.Identifier{ref.Table}.Sanitize()+` ADD CONSTRAINT `+constraint+` FOREIGN KEY (`+pgx.Identifier{ref.Column}.Sanitize()+`) REFERENCES `+pgx.Identifier{ref.Target}.Sanitize()+` (`+pgx.Identifier{ref.TargetColumn}.Sanitize()+`)`)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err := db.Exec(t.Context(), `ALTER TABLE community_invite ADD COLUMN status text; ALTER TABLE community_invite ADD COLUMN resolved_at timestamptz;
	 ALTER TABLE newsfeed_post ADD COLUMN deleted_at timestamptz;
	 ALTER TABLE entity_report ADD COLUMN target_type text; ALTER TABLE entity_report ADD COLUMN target_id text;`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFinalizePostgresRemovesOnlyScopeAndKeepsRequiredBareIdentity(t *testing.T) {
	db := scopeDB(t)
	completeInventoryFixture(t, db)
	ctx := t.Context()
	stamp := time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)
	_, err := db.Exec(ctx, `INSERT INTO entity_entity VALUES ('root-e','realm'),('child-e','realm'),('outside-e','realm'),('person','user');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at,created_by_id)
	 VALUES ('root','root','root-e','server',$1,'person'),('child','child','child-e','channel',NULL,'person'),('outside','outside','outside-e','server',NULL,'root-e');
	 UPDATE community_realm SET parent_id='root' WHERE id='child';
	 INSERT INTO entity_role VALUES (1,'root','owner',TRUE),(2,'child','member',TRUE),(3,'outside','owner',TRUE);
	 INSERT INTO community_member(entity_id,realm_id,role_id,added_by_id) VALUES ('person','root',1,'person'),('person','child',2,'person'),('person','outside',3,'root-e');
	 INSERT INTO entity_role_permissions(id,role_id) VALUES ('grant',1);
	 INSERT INTO community_invite(id,realm_id,role_id,status,created_by_id) VALUES ('invite','child',2,'pending','person');
	 INSERT INTO community_approvals(id,realm_id,actor_id) VALUES ('review','root','person');
	 INSERT INTO entity_token(id,entity_id,realm_id) VALUES ('scoped-token','person','child'),('personal-token','person',NULL);
	 INSERT INTO notes_entry(id,entity_id) VALUES ('owned-note','root-e'),('personal-note','person');
	 INSERT INTO notes_attachment(id,note_id) VALUES ('owned-file','owned-note'),('personal-file','personal-note');
	 INSERT INTO entity_encryption_configuration(id,entity_id) VALUES ('owned-key','root-e'),('personal-key','person');
	 UPDATE entity_encryption_configuration SET replaces_id='owned-key' WHERE id='personal-key';
	 INSERT INTO entity_encryption_configuration_purposes(id,encryptionconfiguration_id) VALUES ('purpose','owned-key');
	 INSERT INTO user_account(id,entity_id) VALUES ('personal-account','person');`, pgx.QueryExecModeSimpleProtocol, stamp)
	if err != nil {
		t.Fatal(err)
	}
	store := SQLStore{DB: db}
	scope, err := store.LoadDeletionScope(ctx, DeletionJob{"root", stamp})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.Finalize(ctx, scope); err != nil {
			t.Fatal(err)
		}
	}
	for _, check := range []struct {
		query string
		want  int
	}{
		{`SELECT COUNT(*) FROM community_realm WHERE id IN ('root','child')`, 0},
		{`SELECT COUNT(*) FROM community_realm WHERE id='outside'`, 1},
		{`SELECT COUNT(*) FROM entity_role WHERE realm_id='outside'`, 1},
		{`SELECT COUNT(*) FROM community_member WHERE realm_id='outside'`, 1},
		{`SELECT COUNT(*) FROM entity_entity WHERE id='root-e' AND type='realm'`, 1},
		{`SELECT COUNT(*) FROM entity_entity WHERE id='child-e'`, 0},
		{`SELECT COUNT(*) FROM notes_entry WHERE id='personal-note'`, 1},
		{`SELECT COUNT(*) FROM notes_entry WHERE id='owned-note'`, 0},
		{`SELECT COUNT(*) FROM entity_encryption_configuration WHERE id='personal-key' AND replaces_id IS NULL`, 1},
		{`SELECT COUNT(*) FROM entity_encryption_configuration WHERE id='owned-key'`, 0},
		{`SELECT COUNT(*) FROM entity_token WHERE id='personal-token'`, 1},
		{`SELECT COUNT(*) FROM entity_token WHERE id='scoped-token'`, 0},
		{`SELECT COUNT(*) FROM user_account WHERE id='personal-account'`, 1},
	} {
		var count int
		if err := db.QueryRow(ctx, check.query).Scan(&count); err != nil || count != check.want {
			t.Fatalf("%s: %d want %d: %v", check.query, count, check.want, err)
		}
	}
}

func TestFinalizePostgresRejectsChangedIntentBeforeDeletingStructure(t *testing.T) {
	db := scopeDB(t)
	completeInventoryFixture(t, db)
	ctx := t.Context()
	stamp := time.Now().UTC().Truncate(time.Microsecond)
	_, err := db.Exec(ctx, `INSERT INTO entity_entity VALUES ('entity','realm');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at) VALUES ('root','root','entity','community',$1)`, pgx.QueryExecModeSimpleProtocol, stamp)
	if err != nil {
		t.Fatal(err)
	}
	store := SQLStore{DB: db}
	if err := store.Finalize(ctx, DeletionScope{Job: DeletionJob{"root", stamp.Add(time.Second)}}); !errors.Is(err, ErrIntentMismatch) {
		t.Fatalf("changed intent accepted: %v", err)
	}
	var exists bool
	if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM community_realm WHERE id='root')`).Scan(&exists); err != nil || !exists {
		t.Fatalf("root changed: %v %v", exists, err)
	}
}

func TestFinalizePostgresRollsBackOnSurvivingForeignKey(t *testing.T) {
	db := scopeDB(t)
	completeInventoryFixture(t, db)
	ctx := t.Context()
	stamp := time.Now().UTC().Truncate(time.Microsecond)
	_, err := db.Exec(ctx, `INSERT INTO entity_entity VALUES ('root-e','realm'),('outside-e','realm'),('person','user');
	 INSERT INTO community_realm(id,realm_id,entity_id,type,deletion_requested_at)
	 VALUES ('root','root','root-e','community',$1),('outside','outside','outside-e','community',NULL);
	 INSERT INTO entity_role VALUES (1,'root','owner',TRUE);
	 INSERT INTO community_member(entity_id,realm_id,role_id) VALUES ('person','root',1),('person','outside',1);`, pgx.QueryExecModeSimpleProtocol, stamp)
	if err != nil {
		t.Fatal(err)
	}
	store := SQLStore{DB: db}
	scope, err := store.LoadDeletionScope(ctx, DeletionJob{"root", stamp})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Finalize(ctx, scope); err == nil {
		t.Fatal("unexpected surviving reference was cascaded away")
	}
	var roots, members int
	if err := db.QueryRow(ctx, `SELECT (SELECT COUNT(*) FROM community_realm),(SELECT COUNT(*) FROM community_member)`).Scan(&roots, &members); err != nil || roots != 2 || members != 2 {
		t.Fatalf("failed transaction changed unrelated structure: %d/%d %v", roots, members, err)
	}
}
