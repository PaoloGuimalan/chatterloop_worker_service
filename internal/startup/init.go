package startup

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"time"
	"worker_service/internal/connections"
	"worker_service/internal/services/rabbitmq"
)

func Init(){
	initialize_connections()
}

func initialize_connections(){
	pgClient := &connections.Postgres{}
	if err := connections.Open(context.Background(), "postgres", pgClient); err != nil {
		log.Fatalf("Critical database initialization failed: %v", err)
	}

	cassClient := &connections.Cassandra{}
	if err := connections.Open(context.Background(), "cassandra", cassClient); err != nil {
		log.Fatalf("Critical database initialization failed: %v", err)
	}

	rmq, err := rabbitmq.RabbitClient()
	if err != nil {
		log.Fatalf("Initialization failed: %v", err)
	}

	initialize_consumers(rmq)
}

func initialize_consumers(rmq *rabbitmq.RabbitMQ){
	slog.Info("Initializing RabbitMQ background consumers...")
	// Every consumer dispatches through rabbitmq.Go rather than a bare `go`, so
	// a panic in one handler is logged with its stack and confined to that
	// message instead of taking the process — and with it all seven listeners —
	// down. Each callback declares its own `payload`, so closing over it is safe.
	rmq.StartListener("update_ranking_score", func(body []byte) {
		var payload rabbitmq.UpdateRankingPayload

		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal JSON payload: %v\n", err)
			return
		}

		rabbitmq.Go("update_ranking_score", func() {
			rabbitmq.UpdateRankingScore(payload.PostID, payload.UpdateType, payload.IsDecrease)
		})
	})

	rmq.StartListener("save_viewcache_engagements", func(body []byte) {
		var payload rabbitmq.ViewCachePayload

		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal view cache JSON payload: %v\n", err)
			return
		}

		rabbitmq.Go("save_viewcache_engagements", func() {
			rabbitmq.SaveViewCacheEngagements(payload.EntityID, payload.ViewCache)
		})
	})

	rmq.StartListener("bump_interest_affinity", func(body []byte) {
		var payload rabbitmq.BumpInterestAffinityPayload

		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal bump interest affinity payload: %v\n", err)
			return
		}

		rabbitmq.Go("bump_interest_affinity", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			rabbitmq.BumpInterestAffinity(ctx, payload.EntityID, payload.InterestIDs, payload.Action, payload.IsDecrease)
		})
	})

	rmq.StartListener("interaction_score_bump", func(body []byte) {
		var payload rabbitmq.InteractionBumpPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal connection interaction payload: %v\n", err)
			return
		}

		rabbitmq.Go("interaction_score_bump", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rabbitmq.InteractionScoreBump(ctx, payload.ActorID, payload.ReceiverID, payload.Action, payload.IsDecrease)
		})
	})

	rmq.StartListener("follower_interaction_score_bump", func(body []byte) {
		var payload rabbitmq.InteractionBumpPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal follower interaction payload: %v\n", err)
			return
		}

		rabbitmq.Go("follower_interaction_score_bump", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rabbitmq.FollowerInteractionScoreBump(ctx, payload.ActorID, payload.ReceiverID, payload.Action, payload.IsDecrease)
		})
	})

	rmq.StartListener("create_post_score_for_new_post", func(body []byte) {
		var payload rabbitmq.NewPostCreatedPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal new post created payload: %v\n", err)
			return
		}

		rabbitmq.Go("create_post_score_for_new_post", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			djangoLayout := "2006-01-02 15:04:05.000 -0700"

			parsedTime, err := time.Parse(djangoLayout, payload.DatePosted)
			if err != nil {
				log.Printf("Failed to parse custom Django datetime string '%s': %v. Defaulting to time.Now()\n", payload.DatePosted, err)
				parsedTime = time.Now()
			}

			rabbitmq.CreatePostScoreForNewPost(ctx, payload.PostID, parsedTime)
		})
	})

	rmq.StartListener("bulk_fanout_to_cache", func(body []byte) {
		var payload rabbitmq.BulkFanoutPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal bulk fanout payload: %v\n", err)
			return
		}

		rabbitmq.Go("bulk_fanout_to_cache", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			rabbitmq.BulkFanoutToCache(ctx, payload.CurrentEntityID, payload.PostData, payload.Type)
		})
	})
}