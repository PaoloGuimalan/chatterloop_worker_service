package connections

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	gocqlastra "github.com/datastax/gocql-astra"
	"github.com/gocql/gocql"
)

type Cassandra struct {
	Session *gocql.Session
}

// requiredEnv reports the names of any variables that are absent or set to an
// empty string, distinguishing the two — a missing line and `KEY=` have
// different causes. Names only; values are never logged.
func requiredEnv(names ...string) []string {
	var problems []string
	for _, name := range names {
		value, present := os.LookupEnv(name)
		switch {
		case !present:
			problems = append(problems, name+" (not set)")
		case value == "":
			problems = append(problems, name+" (set but empty)")
		}
	}
	return problems
}

func (c *Cassandra) Connect(ctx context.Context) error {
	// Named individually because this image is distroless: there is no shell to
	// exec into and inspect the environment with, so this log line is the only
	// diagnostic available in a deployed container.
	if missing := requiredEnv(
		"CASSANDRA_DB_BUNDLE",
		"CASSANDRA_DB_TOKEN",
		"CASSANDRA_DB_KEYSPACE",
	); len(missing) > 0 {
		return fmt.Errorf("cassandra config incomplete: %s", strings.Join(missing, ", "))
	}

	bundlePath := os.Getenv("CASSANDRA_DB_BUNDLE")
	token := os.Getenv("CASSANDRA_DB_TOKEN")
	keyspace := os.Getenv("CASSANDRA_DB_KEYSPACE")

	cluster, err := gocqlastra.NewClusterFromBundle(bundlePath, "token", token, 10*time.Second)
	if err != nil {
		return fmt.Errorf("failed to process cloud secure connect bundle: %w", err)
	}

	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to create astra db cluster session pool: %w", err)
	}

	c.Session = session
	ActiveCassandra = c 
	return nil
}

func (c *Cassandra) Ping(ctx context.Context) error {
	if c.Session == nil {
		return fmt.Errorf("cassandra session is uninitialized")
	}
	
	query := "SELECT cluster_name FROM system.local"
	
	err := c.Session.Query(query).WithContext(ctx).Exec()
	if err != nil {
		return fmt.Errorf("cassandra ping failed: %w", err)
	}
	
	return nil
}

func CassandraSession() *gocql.Session {
	cas, ok := ActiveCassandra.(*Cassandra)
	if !ok || cas == nil {
		return nil
	}
	return cas.Session
}

func (c *Cassandra) Close() {
	if c.Session != nil {
		c.Session.Close()
	}
}
