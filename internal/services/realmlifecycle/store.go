package realmlifecycle

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Both the existing pool and a pinned transaction implement these operations.
type SQLDB interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Begin(context.Context) (pgx.Tx, error)
}

type SQLStore struct{ DB SQLDB }

type ScopeStore interface {
	LoadDeletionScope(context.Context, DeletionJob) (DeletionScope, error)
	PostBatch(context.Context, DeletionScope, string) ([]string, error)
}
