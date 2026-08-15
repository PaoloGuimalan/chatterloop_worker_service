package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"worker_service/internal/connections"
	"worker_service/internal/endpoints"
	"worker_service/internal/logger"
	"worker_service/internal/middlewares"
	"worker_service/internal/services/rabbitmq"

	"github.com/joho/godotenv"
)

func main(){
	logger.Setup(slog.LevelInfo)
	godotenv.Load()

	initialize_connections()
	defer connections.Active.Close()
	defer rabbitmq.ActiveRabbitMQ.Close()
	
	mux := http.NewServeMux()
	mux.HandleFunc("/health", endpoints.HealthCheckHandler)
	mux.HandleFunc("/status", endpoints.DatabaseStatusHandler)

	const art = `


	 ██████╗██╗  ██╗ █████╗ ████████╗████████╗███████╗██████╗ ██╗       █████╗   █████╗  ██████╗ 
	██╔════╝██║  ██║██╔══██╗╚══██╔══╝╚══██╔══╝██╔════╝██╔══██╗██║     ██╔═══██╗██╔═══██╗██╔══██╗
	██║     ███████║███████║   ██║      ██║   █████╗  ██████╔╝██║     ██║   ██║██║   ██║██████╔╝
	██║     ██╔══██║██╔══██║   ██║      ██║   ██╔══╝  ██╔══██╗██║     ██║   ██║██║   ██║██╔═══╝ 
	╚██████╗██║  ██║██║  ██║   ██║      ██║   ███████╗██║  ██║███████╗╚██████╔╝╚██████╔╝██║     
	╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚══════╝╚═╝  ╚═╝╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     


	`

	log.Println(art)

	log.Println("🚀 API Server started on http://localhost:8880")

	if err := http.ListenAndServe(":8880", middlewares.Requests(mux)); err != nil {
		slog.Error("server stopped", "err", err)
	}
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
		go fmt.Println(string(body))
	})
}
