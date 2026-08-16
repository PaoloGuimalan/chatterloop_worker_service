package startup

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"worker_service/internal/connections"
	"worker_service/internal/services/rabbitmq"
)

func Init(){
	initialize_connections()
}

func initialize_connections(){
	if _, err := connections.Open(context.Background()); err != nil {
		log.Fatal(err)
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
		go fmt.Println(string(body))
	})
}