package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"worker_service/internal/connections"
	"worker_service/internal/endpoints"
	"worker_service/internal/logger"
	"worker_service/internal/middlewares"
)

func main(){
	logger.Setup(slog.LevelInfo)

	initialize_connections()
	defer connections.Active.Close()
	
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
}
