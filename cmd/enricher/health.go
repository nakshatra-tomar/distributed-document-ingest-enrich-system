package main

import (
    "context"
    "encoding/json"
    "net/http"
    "time"

    "github.com/sirupsen/logrus"
)

// HealthChecker exposes /health, /ready, /status, /metrics for the Enricher service.
type HealthChecker struct {
    service *EnricherService
    logger  *logrus.Logger
}

// HealthStatus is the payload returned by /health.
type HealthStatus struct {
    Status    string                 `json:"status"`
    Timestamp time.Time              `json:"timestamp"`
    Version   string                 `json:"version"`
    Uptime    string                 `json:"uptime"`
    Service   string                 `json:"service"`
    Metrics   map[string]interface{} `json:"metrics"`
}

// NewHealthChecker wires a HealthChecker for the given service.
func NewHealthChecker(service *EnricherService, logger *logrus.Logger) *HealthChecker {
    return &HealthChecker{service: service, logger: logger}
}

// StartHealthServer runs the HTTP server with sane timeouts and graceful shutdown.
func (h *HealthChecker) StartHealthServer(ctx context.Context, port string) {
    mux := http.NewServeMux()
    mux.HandleFunc("/health", h.healthHandler)
    mux.HandleFunc("/metrics", h.metricsHandler)
    mux.HandleFunc("/ready", h.readinessHandler)
    mux.HandleFunc("/status", h.statusHandler)

    srv := &http.Server{
        Addr:              ":" + port,
        Handler:           mux,
        ReadTimeout:       10 * time.Second,
        ReadHeaderTimeout: 5 * time.Second,
        WriteTimeout:      10 * time.Second,
        IdleTimeout:       60 * time.Second,
    }

    h.logger.WithField("port", port).Info("starting health server")

    // Graceful shutdown: stop accepting new conns, wait for in-flight to complete.
    go func() {
        <-ctx.Done()
        shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        if err := srv.Shutdown(shutdownCtx); err != nil && err != http.ErrServerClosed {
            h.logger.WithError(err).Error("health server shutdown error")
        }
    }()

    if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
        h.logger.WithError(err).Error("health server failed")
    }
}

// healthHandler returns a quick “am I alive” payload plus a metrics snapshot.
func (h *HealthChecker) healthHandler(w http.ResponseWriter, r *http.Request) {
    h.service.metrics.mu.RLock()
    defer h.service.metrics.mu.RUnlock()

    status := HealthStatus{
        Status:    "healthy",
        Timestamp: time.Now(),
        Version:   "1.0.0",
        Service:   "enricher",
        Uptime:    time.Since(h.service.metrics.StartTime).String(),
        Metrics:   h.getMetrics(),
    }

    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(status); err != nil {
        h.logger.WithError(err).Error("encode /health response")
        http.Error(w, "internal server error", http.StatusInternalServerError)
    }
}

// metricsHandler returns raw metrics as JSON (handy for dashboards).
func (h *HealthChecker) metricsHandler(w http.ResponseWriter, r *http.Request) {
    h.service.metrics.mu.RLock()
    defer h.service.metrics.mu.RUnlock()

    m := h.getMetrics()

    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(m); err != nil {
        h.logger.WithError(err).Error("encode /metrics response")
        http.Error(w, "internal server error", http.StatusInternalServerError)
        return
    }
}

// readinessHandler distinguishes “alive” vs “ready to serve traffic”.
func (h *HealthChecker) readinessHandler(w http.ResponseWriter, r *http.Request) {
    h.service.metrics.mu.RLock()
    defer h.service.metrics.mu.RUnlock()

    // Ready if we processed at least one doc OR have been up > 30s.
    isReady := h.service.metrics.ProcessedCount > 0 ||
        time.Since(h.service.metrics.StartTime) > 30*time.Second

    w.Header().Set("Content-Type", "application/json")
    if isReady {
        w.WriteHeader(http.StatusOK)
        resp := map[string]interface{}{
            "status":          "ready",
            "processed_count": h.service.metrics.ProcessedCount,
            "uptime":          time.Since(h.service.metrics.StartTime).String(),
        }
        _ = json.NewEncoder(w).Encode(resp)
        return
    }

    w.WriteHeader(http.StatusServiceUnavailable)
    resp := map[string]interface{}{
        "status": "not ready",
        "uptime": time.Since(h.service.metrics.StartTime).String(),
        "reason": "service still starting up",
    }
    _ = json.NewEncoder(w).Encode(resp)
}

// statusHandler returns a richer operational snapshot: error rate, throughput, etc.
func (h *HealthChecker) statusHandler(w http.ResponseWriter, r *http.Request) {
    h.service.metrics.mu.RLock()
    defer h.service.metrics.mu.RUnlock()

    uptime := time.Since(h.service.metrics.StartTime)
    var throughput float64
    if s := uptime.Seconds(); s > 0 {
        throughput = float64(h.service.metrics.ProcessedCount) / s
    }

    total := h.service.metrics.ProcessedCount + h.service.metrics.ErrorCount
    var errRate float64
    if total > 0 {
        errRate = float64(h.service.metrics.ErrorCount) / float64(total)
    }

    status := map[string]interface{}{
        "service":               "enricher",
        "version":               "1.0.0",
        "status":                "running",
        "uptime":                uptime.String(),
        "processed_count":       h.service.metrics.ProcessedCount,
        "error_count":           h.service.metrics.ErrorCount,
        "error_rate":            errRate,
        "throughput_per_second": throughput,
        "avg_processing_time_ms": h.service.metrics.AverageProcessingTime,
        "last_processed_at":     h.service.metrics.LastProcessedAt,
        "documents_by_type":     h.service.metrics.DocumentsByType,
        "start_time":            h.service.metrics.StartTime,
    }

    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(status); err != nil {
        h.logger.WithError(err).Error("encode /status response")
        http.Error(w, "internal server error", http.StatusInternalServerError)
    }
}

// getMetrics composes the shared metrics map used by /health and /metrics.
// NOTE: caller must already hold the read lock.
func (h *HealthChecker) getMetrics() map[string]interface{} {
    uptime := time.Since(h.service.metrics.StartTime)

    var throughput float64
    if s := uptime.Seconds(); s > 0 {
        throughput = float64(h.service.metrics.ProcessedCount) / s
    }

    total := h.service.metrics.ProcessedCount + h.service.metrics.ErrorCount
    var errRate float64
    if total > 0 {
        errRate = float64(h.service.metrics.ErrorCount) / float64(total)
    }

    return map[string]interface{}{
        "processed_count":        h.service.metrics.ProcessedCount,
        "error_count":            h.service.metrics.ErrorCount,
        "start_time":             h.service.metrics.StartTime,
        "last_processed_at":      h.service.metrics.LastProcessedAt,
        "avg_processing_time_ms": h.service.metrics.AverageProcessingTime,
        "total_processing_time":  h.service.metrics.TotalProcessingTime.String(),
        "uptime_seconds":         uptime.Seconds(),
        "throughput_per_second":  throughput,
        "error_rate":             errRate,
        "documents_by_type":      h.service.metrics.DocumentsByType,
    }
}
