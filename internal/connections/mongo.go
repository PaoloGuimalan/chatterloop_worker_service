package connections

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Mongo holds the session store. Push notifications are addressed to PEOPLE but
// FCM only accepts device tokens, and the mapping between them - along with
// whether each device currently has a live SSE connection - lives in the
// `sessions` collection. Sending a push therefore means reading it, and
// retiring a token FCM has rejected means writing back to it.
type Mongo struct {
	Client   *mongo.Client
	Database *mongo.Database
}

// Named individually rather than taking one connection string, matching how
// every other backend in this service is configured (DB_*, CASSANDRA_DB_*,
// RABBITMQ_*) - and so the missing-variable error can say which one.
func (m *Mongo) uri() (string, error) {
	host := os.Getenv("MONGODB_CLUSTER_HOST")
	user := os.Getenv("MONGODB_CLUSTER_USER")
	password := os.Getenv("MONGODB_CLUSTER_PASS")

	var missing []string
	if host == "" {
		missing = append(missing, "MONGODB_CLUSTER_HOST")
	}
	if user == "" {
		missing = append(missing, "MONGODB_CLUSTER_USER")
	}
	if password == "" {
		missing = append(missing, "MONGODB_CLUSTER_PASS")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("mongo config incomplete: missing %s", strings.Join(missing, ", "))
	}

	// Credentials are escaped: a password containing @ / : or ? would otherwise
	// be read as part of the host or the query string.
	return fmt.Sprintf(
		"mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority",
		url.QueryEscape(user),
		url.QueryEscape(password),
		host,
	), nil
}

func (m *Mongo) Connect(ctx context.Context) error {
	uri, err := m.uri()
	if err != nil {
		return err
	}

	name := os.Getenv("MONGODB_DB")
	if name == "" {
		return fmt.Errorf("mongo config incomplete: missing MONGODB_DB")
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return err
	}

	m.Client = client
	m.Database = client.Database(name)
	return nil
}

func (m *Mongo) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return m.Client.Ping(ctx, nil)
}

func (m *Mongo) Close() {
	if m.Client != nil {
		_ = m.Client.Disconnect(context.Background())
	}
}

// Sessions is the collection holding one row per signed-in device.
//
// Named explicitly rather than derived: the Node service declares it through
// mongoose (server/schema/auth/sessions.js), which pluralises the model name,
// so the physical name is not something either side should be guessing.
func Sessions() *mongo.Collection {
	mg, ok := ActiveMongo.(*Mongo)
	if !ok || mg.Database == nil {
		return nil
	}
	return mg.Database.Collection("sessions")
}
