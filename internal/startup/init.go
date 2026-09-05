package startup

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"
	"worker_service/internal/connections"
	"worker_service/internal/services/rabbitmq"
)

func Init() {
	initialize_connections()
}

func initialize_connections() {
	pgClient := &connections.Postgres{}
	if err := connections.Open(context.Background(), "postgres", pgClient); err != nil {
		log.Fatalf("Critical database initialization failed: %v", err)
	}

	cassClient := &connections.Cassandra{}
	if err := connections.Open(context.Background(), "cassandra", cassClient); err != nil {
		log.Fatalf("Critical database initialization failed: %v", err)
	}

	mongoClient := &connections.Mongo{}
	if err := connections.Open(context.Background(), "mongo", mongoClient); err != nil {
		log.Printf("Mongo unavailable, push notifications will be dropped: %v", err)
	}

	rmq, err := rabbitmq.RabbitClient()
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}

	initialize_consumers(rmq)
}

// handle adapts a typed worker function into a rabbitmq.HandlerFunc. Malformed
// JSON is permanent, so it is reported as ErrDrop and acked away instead of
// being requeued forever.
func handle[T any](fn func(ctx context.Context, payload T)) rabbitmq.HandlerFunc {
	return func(ctx context.Context, body []byte) error {
		var payload T
		if err := json.Unmarshal(body, &payload); err != nil {
			return fmt.Errorf("%w: %v", rabbitmq.ErrDrop, err)
		}

		fn(ctx, payload)

		return nil
	}
}

// initialize_consumers registers every queue subscription, then starts them
// together. Timeout is the per-message deadline; Workers caps how many messages
// from that queue run at once, which keeps a large backlog from stampeding
// Postgres, Astra and Mongo on restart.
func initialize_consumers(rmq *rabbitmq.RabbitMQ) {
	slog.Info("Initializing RabbitMQ background consumers...")

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "update_ranking_score",
		Timeout: 10 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.UpdateRankingPayload) {
			rabbitmq.UpdateRankingScore(p.PostID, p.UpdateType, p.IsDecrease)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "save_viewcache_engagements",
		Timeout: 30 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.ViewCachePayload) {
			rabbitmq.SaveViewCacheEngagements(p.EntityID, p.ViewCache)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "bump_interest_affinity",
		Timeout: 5 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.BumpInterestAffinityPayload) {
			rabbitmq.BumpInterestAffinity(ctx, p.EntityID, p.InterestIDs, p.Action, p.IsDecrease)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "interaction_score_bump",
		Timeout: 5 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.InteractionBumpPayload) {
			rabbitmq.InteractionScoreBump(ctx, p.ActorID, p.ReceiverID, p.Action, p.IsDecrease)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "follower_interaction_score_bump",
		Timeout: 5 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.InteractionBumpPayload) {
			rabbitmq.FollowerInteractionScoreBump(ctx, p.ActorID, p.ReceiverID, p.Action, p.IsDecrease)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "create_post_score_for_new_post",
		Timeout: 5 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.NewPostCreatedPayload) {
			parsedTime := time.Now()
			if p.DatePosted != "" {
				if t, err := time.Parse(time.RFC3339, p.DatePosted); err == nil {
					parsedTime = t
				} else {
					log.Printf("Failed to parse date_posted '%s': %v. Defaulting to time.Now()\n", p.DatePosted, err)
				}
			}

			rabbitmq.CreatePostScoreForNewPost(ctx, p.PostID, parsedTime)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "bulk_fanout_to_cache",
		Timeout: 10 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.BulkFanoutPayload) {
			rabbitmq.BulkFanoutToCache(ctx, p.CurrentEntityID, p.PostData, p.Type)
		}),
	})

	// Fan-out over a whole friend graph is the heaviest job here, so it runs with
	// a small window to avoid saturating Astra.
	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:    "backfill_new_friend_feed",
		Timeout:  20 * time.Second,
		Prefetch: 4,
		Workers:  2,
		Handler: handle(func(ctx context.Context, p rabbitmq.BackfillFriendFeedPayload) {
			rabbitmq.BackfillNewFriendFeed(ctx, p.ViewerID, p.NewFriendID, p.Type)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "send_push",
		Timeout: 30 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.SendPushPayload) {
			rabbitmq.SendPush(ctx, p)
		}),
	})

	// SMTP is slow and the relay dislikes parallel sessions.
	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:    "send_email",
		Timeout:  30 * time.Second,
		Prefetch: 4,
		Workers:  2,
		Handler: handle(func(ctx context.Context, p rabbitmq.SendEmailPayload) {
			rabbitmq.SendEmail(p.To, p.From, p.Subject, p.Body)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "remove_engagement_log",
		Timeout: 10 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.RemoveEngagementLogPayload) {
			rabbitmq.RemoveEngagementLog(ctx, p.EntityID, p.ActivityType, p.TargetType, p.TargetID)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "bump_chat_score",
		Timeout: 5 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.ChatScoreBumpPayload) {
			rabbitmq.BumpChatScore(ctx, p.ActorID, p.MemberIDs, p.Action, p.IsDecrease)
		}),
	})

	rmq.Register(rabbitmq.ConsumerConfig{
		Queue:   "remove_feed_on_unfriend",
		Timeout: 10 * time.Second,
		Handler: handle(func(ctx context.Context, p rabbitmq.RemoveFeedPayload) {
			rabbitmq.RemoveFeedOnUnfriend(ctx, p.ActorID, p.AuthorID, p.Type)
		}),
	})

	// Fatal on purpose: a worker that boots without consumers looks healthy while
	// every queue silently backs up.
	if err := rmq.Start(); err != nil {
		log.Fatalf("Failed to start RabbitMQ consumers: %v", err)
	}
}
