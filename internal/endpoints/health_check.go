package endpoints

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"worker_service/internal/connections"
)

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{"status": "alive", "service": "worker_service"}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("health check encode failed", "err", err)
	}
}

// DatabaseStatusHandler checks the live database connection and fetches the time
func DatabaseStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	start := time.Now()

	var dbTime time.Time
	if err := connections.Pool().QueryRow(ctx, "SELECT NOW()").Scan(&dbTime); err != nil {
		slog.Error("database status failed",
			"err", err,
			"took", time.Since(start).Round(time.Millisecond),
		)

		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Database query failed"})
		return
	}

	slog.Info("database status ok",
		"db_time", dbTime.Format(time.RFC3339),
		"took", time.Since(start).Round(time.Millisecond),
	)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"database": "connected",
		"db_time":  dbTime.String(),
	})
}