package main

import (
	"log"
	"log/slog"
	"net/http"
	"worker_service/internal/connections"
	"worker_service/internal/endpoints"
	"worker_service/internal/logger"
	"worker_service/internal/middlewares"
	"worker_service/internal/services/rabbitmq"
	"worker_service/internal/startup"

	"github.com/joho/godotenv"
)

func main(){
	logger.Setup(slog.LevelInfo)
	godotenv.Load()

	const art = `


	 ██████╗██╗  ██╗ █████╗ ████████╗████████╗███████╗██████╗ ██╗       █████╗   █████╗  ██████╗ 
	██╔════╝██║  ██║██╔══██╗╚══██╔══╝╚══██╔══╝██╔════╝██╔══██╗██║     ██╔═══██╗██╔═══██╗██╔══██╗
	██║     ███████║███████║   ██║      ██║   █████╗  ██████╔╝██║     ██║   ██║██║   ██║██████╔╝
	██║     ██╔══██║██╔══██║   ██║      ██║   ██╔══╝  ██╔══██╗██║     ██║   ██║██║   ██║██╔═══╝ 
	╚██████╗██║  ██║██║  ██║   ██║      ██║   ███████╗██║  ██║███████╗╚██████╔╝╚██████╔╝██║     
	╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚══════╝╚═╝  ╚═╝╚══════╝ ╚═════╝  ╚═════╝ ╚═╝     


	`

	log.Println(art)

	startup.Init()
	defer connections.Active.Close()
	defer rabbitmq.ActiveRabbitMQ.Close()
	
	mux := http.NewServeMux()
	mux.HandleFunc("/health", endpoints.HealthCheckHandler)
	mux.HandleFunc("/status", endpoints.DatabaseStatusHandler)

	log.Println("🚀 API Server started on http://localhost:8880")

	if err := http.ListenAndServe(":8880", middlewares.Requests(mux)); err != nil {
		slog.Error("server stopped", "err", err)
	}
}
