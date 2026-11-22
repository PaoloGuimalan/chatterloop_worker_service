package realmlifecycle

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type indexRowsFixture struct {
	rows       [][]any
	closeError error
}

func (r *indexRowsFixture) Scan(dest ...any) bool {
	if len(r.rows) == 0 {
		return false
	}
	row := r.rows[0]
	r.rows = r.rows[1:]
	for i, value := range row {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(value))
	}
	return true
}
func (r *indexRowsFixture) Close() error { return r.closeError }

type indexFixture struct {
	batches    [][][]any
	fail       bool
	closeError error
}

func (s *indexFixture) Iter(_ context.Context, sql string, _ ...any) cqlRows {
	rows := &indexRowsFixture{closeError: s.closeError}
	if strings.Contains(sql, "newsfeed_index") {
		for i := 0; i < 205; i++ {
			rows.rows = append(rows.rows, []any{"viewer", "post", time.Unix(int64(i), 0)})
		}
	}
	return rows
}
func (s *indexFixture) DeleteBatch(_ context.Context, _ string, keys [][]any) error {
	if s.fail {
		return errors.New("Cassandra unavailable")
	}
	s.batches = append(s.batches, keys)
	return nil
}

func TestIndexCleanupUsesExactKeysAndBoundedBatches(t *testing.T) {
	store := &indexFixture{}
	if err := deletePostIndexes(t.Context(), store, "post"); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, batch := range store.batches {
		if len(batch) > 100 {
			t.Fatal("unbounded Cassandra deletion")
		}
		for _, key := range batch {
			if key[0] != "viewer" || key[1] != "post" {
				t.Fatalf("wrong feed key: %v", key)
			}
		}
		count += len(batch)
	}
	if count != 205 {
		t.Fatalf("missed index rows: %d", count)
	}
	for _, failure := range []*indexFixture{{fail: true}, {closeError: errors.New("scan failed")}} {
		if err := deletePostIndexes(t.Context(), failure, "post"); err == nil {
			t.Fatal("index dependency failure swallowed")
		}
	}
}
