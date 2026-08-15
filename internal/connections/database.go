package connections

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Database interface {
	Connect(ctx context.Context) error
	Ping(ctx context.Context) error
	Close()
}

var Active Database

// Open builds the driver named by DB_DRIVER and connects it.
func Open(ctx context.Context) (Database, error) {
	godotenv.Load()
	driver := os.Getenv("DB_DRIVER")
	if driver == "" {
		driver = "postgres"
	}

	var db Database
	switch driver {
	case "postgres":
		db = &Postgres{}
	// case "mysql":
	//     db = &MySQL{}
	default:
		return nil, fmt.Errorf("unknown DB_DRIVER %q", driver)
	}

	if err := db.Connect(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", driver, err)
	}

		if err := db.Connect(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", driver, err)
	}

	log.Printf("Database (%v) connected", driver)

	Active = db
	return db, nil
}