package realmlifecycle

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// This is a cleanup inventory, not another permission or lifecycle datastore.
//
//go:embed schema_inventory.json
var inventoryJSON []byte

var ErrUnknownReference = errors.New("unclassified realm cleanup reference")

type schemaReference struct {
	Table        string `json:"table"`
	Column       string `json:"column"`
	Target       string `json:"target"`
	TargetColumn string `json:"target_column"`
	Policy       string `json:"policy"`
}

func (r schemaReference) key() string {
	return r.Table + "." + r.Column + "->" + r.Target + "." + r.TargetColumn
}

// Called only by permanent cleanup, never during an ordinary user request.
// Unknown references stop deletion rather than guessing a cascade policy.
func (s SQLStore) CheckInventory(ctx context.Context) error {
	var manifest []schemaReference
	if err := json.Unmarshal(inventoryJSON, &manifest); err != nil {
		return err
	}
	known, targets := map[string]bool{}, map[string]bool{}
	for _, ref := range manifest {
		if ref.Policy == "" {
			return fmt.Errorf("%w: %s", ErrUnknownReference, ref.key())
		}
		known[ref.key()], targets[ref.Target] = true, true
	}
	names := make([]string, 0, len(targets))
	for target := range targets {
		names = append(names, target)
	}
	sort.Strings(names)
	rows, err := s.DB.Query(ctx, `SELECT source.relname, a.attname, target.relname, b.attname
	 FROM pg_constraint c JOIN pg_class source ON source.oid=c.conrelid
	 JOIN pg_class target ON target.oid=c.confrelid
	 JOIN pg_namespace n ON n.oid=target.relnamespace
	 CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS k(attnum, position)
	 JOIN pg_attribute a ON a.attrelid=source.oid AND a.attnum=k.attnum
	 JOIN pg_attribute b ON b.attrelid=target.oid AND b.attnum=c.confkey[k.position]
	 WHERE c.contype='f' AND target.relname=ANY($1::text[])
	 AND n.nspname=ANY(current_schemas(FALSE))`, names)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		Progress(ctx)
		var ref schemaReference
		if err := rows.Scan(&ref.Table, &ref.Column, &ref.Target, &ref.TargetColumn); err != nil {
			return err
		}
		if !known[ref.key()] {
			return fmt.Errorf("%w: %s", ErrUnknownReference, ref.key())
		}
	}
	return rows.Err()
}
