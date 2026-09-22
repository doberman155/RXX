package httpx

import (
	"context"
	"net/http"
	"time"
)

// HealthResponse — тело ответа /healthz и /readyz.
type HealthResponse struct {
	Status  string `json:"status"`
	Service string `json:"service"`
	Version string `json:"version,omitempty"`
}

// Probe — проверка готовности зависимости (например, ping к PostgreSQL).
type Probe func(ctx context.Context) error

// Health отвечает 200, пока процесс жив. Зависимости не проверяет,
// поэтому пригоден для liveness-пробы.
func Health(service string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		JSON(w, r, http.StatusOK, HealthResponse{Status: "ok", Service: service})
	}
}

// Ready проверяет зависимости и отвечает 503, если хотя бы одна недоступна.
func Ready(service string, probes ...Probe) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		for _, probe := range probes {
			if err := probe(ctx); err != nil {
				FromRequest(r).Warn("readiness-проба не пройдена", "error", err.Error())
				Error(w, r, http.StatusServiceUnavailable, "not_ready", "зависимости сервиса недоступны")
				return
			}
		}
		JSON(w, r, http.StatusOK, HealthResponse{Status: "ready", Service: service})
	}
}
