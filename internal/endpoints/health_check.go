package endpoints

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"worker_service/internal/connections"
	"worker_service/internal/services/rabbitmq"
)

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{"status": "alive", "service": "worker_service"}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("health check encode failed", "err", err)
	}
}

// StatusHandler reports readiness across every dependency the worker needs.
// RabbitMQ is included deliberately: this process serves HTTP whether or not its
// consumers are attached, so without this check a total consumption outage looks
// perfectly healthy to orchestration while queues back up.
func StatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	start := time.Now()
	healthy := true

	body := map[string]any{}

	var dbTime time.Time
	if err := connections.Pool().QueryRow(ctx, "SELECT NOW()").Scan(&dbTime); err != nil {
		slog.Error("database status failed",
			"err", err,
			"took", time.Since(start).Round(time.Millisecond),
		)

		healthy = false
		body["database"] = "unreachable"
	} else {
		body["database"] = "connected"
		body["db_time"] = dbTime.String()
	}

	if rabbitmq.ActiveRabbitMQ.Healthy() {
		body["rabbitmq"] = "connected"
		body["consumers"] = rabbitmq.ActiveRabbitMQ.Consumers()
	} else {
		slog.Error("rabbitmq status failed", "reason", "no live connection")

		healthy = false
		body["rabbitmq"] = "disconnected"
		body["consumers"] = 0
	}

	if !healthy {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(body)
		return
	}

	slog.Info("status ok",
		"db_time", dbTime.Format(time.RFC3339),
		"consumers", rabbitmq.ActiveRabbitMQ.Consumers(),
		"took", time.Since(start).Round(time.Millisecond),
	)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(body)
}
