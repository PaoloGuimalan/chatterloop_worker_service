package connections

import (
	"context"
	"fmt"
	"log"
)

type Database interface {
	Connect(ctx context.Context) error
	Ping(ctx context.Context) error
	Close()
}

var Active Database
var ActiveCassandra Database

var registry = make(map[string]Database)

func Open(ctx context.Context, name string, db Database) error {
	if err := db.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect %s: %w", name, err)
	}

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping %s: %w", name, err)
	}

	log.Printf("Database backend (%s) successfully connected and verified", name)

	registry[name] = db

	switch name {
		case "postgres":
			Active = db
		case "cassandra":
			ActiveCassandra = db
	}

	return nil
}

func CloseAll() {
	for name, db := range registry {
		log.Printf("Closing database connection: %s", name)
		db.Close()
	}
}
