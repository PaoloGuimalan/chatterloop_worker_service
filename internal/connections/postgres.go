package connections

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	Pool *pgxpool.Pool
}

func (p *Postgres) url() (string, error) {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	user := os.Getenv("DB_USERNAME")
	password := os.Getenv("DB_PASSWORD")

	if host == "" || port == "" || name == "" || user == "" || password == "" {
		return "", fmt.Errorf("missing one or more DB_ environment variables")
	}

	query := url.Values{}
	query.Set("sslmode", "require")

	// Supabase's transaction pooler (6543) runs pgbouncer, which does not
	// support the prepared statements pgx caches by default.
	if port == "6543" {
		query.Set("default_query_exec_mode", "simple_protocol")
	}

	dsn := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     net.JoinHostPort(host, port),
		Path:     name,
		RawQuery: query.Encode(),
	}

	return dsn.String(), nil
}

func (p *Postgres) Connect(ctx context.Context) error {
	connString, err := p.url()
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to create database pool: %w", err)
	}

	// 10s not 3s: warm connect is ~300ms, but the first connect pays DNS + TLS
	// + pooler cold start and measured 1.4s here.
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping database: %w", err)
	}

	p.Pool = pool
	return nil
}

func (p *Postgres) Ping(ctx context.Context) error { return p.Pool.Ping(ctx) }

func Pool() *pgxpool.Pool {
	pg, ok := Active.(*Postgres)
	if !ok {
		return nil
	}
	return pg.Pool
}

func (p *Postgres) Close() {
	if p.Pool != nil {
		p.Pool.Close()
	}
}