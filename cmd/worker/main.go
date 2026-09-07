package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"worker_service/internal/connections"
	"worker_service/internal/endpoints"
	"worker_service/internal/logger"
	"worker_service/internal/middlewares"
	"worker_service/internal/services/rabbitmq"
	"worker_service/internal/startup"

	"github.com/joho/godotenv"
)

const shutdownTimeout = 30 * time.Second

func main() {
	logger.Setup(slog.LevelInfo)
	godotenv.Load()

	const art = `


	 ██████╗██╗  ██╗ █████╗ ████████╗████████╗███████╗██████╗ ██╗       █████╗   █████╗  █████╗
	██╔════╝██║  ██║██╔══██╗╚══██╔══╝╚══██╔══╝██╔════╝██╔══██╗██║     ██╔═══██╗██╔═══██╗██╔══██╗
	██║     ███████║███████║   ██║      ██║   █████╗  ██████╔╝██║     ██║   ██║██║   ██║██████╔╝
	██║     ██╔══██║██╔══██║   ██║      ██║   ██╔══╝  ██╔══██╗██║     ██║   ██║██║   ██║██╔═══╝
	╚██████╗██║  ██║██║  ██║   ██║      ██║   ███████╗██║  ██║███████╗╚██████╔╝╚██████╔╝██║
	 ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝      ╚═╝   ╚══════╝╚═╝  ╚═╝╚══════╝ ╚═════╝  ╚═════╝ ╚═╝


	`

	log.Println(art)

	startup.Init()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", endpoints.HealthCheckHandler)
	mux.HandleFunc("/status", endpoints.StatusHandler)
	mux.HandleFunc("/version", endpoints.VersionHandler)

	server := &http.Server{
		Addr:    ":8880",
		Handler: middlewares.Requests(mux),
	}

	// SIGTERM is what the orchestrator sends on redeploy. Catching it lets
	// in-flight handlers finish so their messages are acked rather than
	// redelivered on the next boot.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Println("🚀 API Server started on http://localhost:8880")

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server stopped", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("Shutdown signal received, draining in-flight work...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown failed", "err", err)
	}

	rabbitmq.ActiveRabbitMQ.Close()
	connections.CloseAll()

	slog.Info("Shutdown complete")
}
