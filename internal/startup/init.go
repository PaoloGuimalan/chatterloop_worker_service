package startup

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"time"
	"worker_service/internal/connections"
	"worker_service/internal/models"
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
	rmq.StartListener("update_ranking_score", func(body []byte) {
		go func(msgBody []byte) {
			var payload rabbitmq.UpdateRankingPayload

			err := json.Unmarshal(msgBody, &payload)
			if err != nil {
				log.Printf("Failed to unmarshal JSON payload: %v\n", err)
				return
			}

			rabbitmq.UpdateRankingScore(payload.PostID, payload.UpdateType, payload.IsDecrease)
		}(body) 
	})

	rmq.StartListener("save_viewcache_engagements", func(body []byte) {
		var payload rabbitmq.ViewCachePayload

		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal view cache JSON payload: %v\n", err)
			return
		}

		go func(entity string, cache []models.ViewCacheItem) {
			rabbitmq.SaveViewCacheEngagements(entity, cache)
		}(payload.EntityID, payload.ViewCache)
	})

	rmq.StartListener("bump_interest_affinity", func(body []byte) {
		var payload rabbitmq.BumpInterestAffinityPayload

		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal bump interest affinity payload: %v\n", err)
			return
		}

		go func(entity string, interests []string, action string, decrease bool) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			rabbitmq.BumpInterestAffinity(ctx, entity, interests, action, decrease)
		}(payload.EntityID, payload.InterestIDs, payload.Action, payload.IsDecrease)
	})

	rmq.StartListener("interaction_score_bump", func(body []byte) {
		var payload rabbitmq.InteractionBumpPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal connection interaction payload: %v\n", err)
			return
		}

		go func(p rabbitmq.InteractionBumpPayload) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rabbitmq.InteractionScoreBump(ctx, p.ActorID, p.ReceiverID, p.Action, p.IsDecrease)
		}(payload)
	})

	rmq.StartListener("follower_interaction_score_bump", func(body []byte) {
		var payload rabbitmq.InteractionBumpPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("Failed to unmarshal follower interaction payload: %v\n", err)
			return
		}

		go func(p rabbitmq.InteractionBumpPayload) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rabbitmq.FollowerInteractionScoreBump(ctx, p.ActorID, p.ReceiverID, p.Action, p.IsDecrease)
		}(payload)
	})
}