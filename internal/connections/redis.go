package connections

import (
	"context"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

// Redis, for publishing realtime frames.
//
// # WHY THIS SERVICE NEEDS IT AT ALL
//
// A built-in command answers in the conversation, and a client only learns
// about a message from a frame on `events_<entity_id>`. That is a Redis
// publish, so a worker that could write the message but not announce it would
// produce a reply nobody sees until they refresh.
//
// # OPTIONAL, AND DEGRADES RATHER THAN FAILS
//
// Every other connection here is required; this one is not. With REDIS_HOST
// unset the service starts, and everything that does not publish frames works
// exactly as before - only built-in command replies go unannounced. Making it
// required would turn "we have not configured Redis yet" into a worker that
// will not boot, taking ranking, feeds and push down with it.
type Redis struct {
	Client *redis.Client
}

func (r *Redis) Connect(ctx context.Context) error {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		// An error so the caller logs "Redis unavailable" rather than Open
		// announcing a backend it never dialled. It is not FATAL - see the
		// type comment and the caller in startup.
		return fmt.Errorf("REDIS_HOST is not set")
	}

	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}

	r.Client = redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		Username: os.Getenv("REDIS_USERNAME"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})
	return nil
}

func (r *Redis) Ping(ctx context.Context) error {
	if r.Client == nil {
		return nil
	}
	return r.Client.Ping(ctx).Err()
}

func (r *Redis) Close() {
	if r.Client != nil {
		_ = r.Client.Close()
	}
}

// ActiveRedis is nil-safe to consult: callers check RedisClient() rather than
// asserting, so an unconfigured deployment takes the quiet path.
var ActiveRedis Database

// RedisClient returns the client, or nil when Redis is not configured.
func RedisClient() *redis.Client {
	rd, ok := ActiveRedis.(*Redis)
	if !ok || rd == nil {
		return nil
	}
	return rd.Client
}
