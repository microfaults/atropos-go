package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/microfaults/atropos-go/loadgen/internal/api"
	"github.com/microfaults/atropos-go/loadgen/internal/attacker"
	"github.com/microfaults/atropos-go/loadgen/internal/dedup"
	"github.com/microfaults/atropos-go/loadgen/internal/policy"
	"github.com/microfaults/atropos-go/loadgen/internal/workload"
)

func main() {
	addr := envOr("ARCHER_ADDR", ":8080")
	evalInterval := 10 * time.Second

	// Wire dependencies.
	registry := workload.NewRegistry()
	manager := attacker.NewManager()

	// Register default dedup bypass strategies.
	manager.RegisterBypass("header", &dedup.HeaderMutator{HeaderName: "X-Idempotency-Key"})
	manager.RegisterBypass("query", &dedup.QueryParamMutator{ParamName: "nonce"})

	// Policy engine with workload-derived metrics.
	metricSource := &RegistryMetricSource{Registry: registry}
	engine := policy.NewEngine(metricSource, manager, evalInterval)

	// HTTP API server.
	server := api.NewServer(registry, manager, engine)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Handler(),
	}

	// Graceful shutdown on SIGINT / SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start policy engine in background.
	go engine.Run(ctx)

	// Start HTTP server.
	go func() {
		log.Printf("archer: listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("archer: server error: %v", err)
		}
	}()

	// Wait for shutdown signal.
	<-ctx.Done()
	log.Println("archer: shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("archer: http shutdown error: %v", err)
	}

	manager.StopAll()
	log.Println("archer: stopped")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// RegistryMetricSource derives metrics from the workload registry.
// Lives in main for now; can move to its own package if it grows.
type RegistryMetricSource struct {
	Registry *workload.Registry
}

func (s *RegistryMetricSource) GetMetric(name string) (float64, bool) {
	switch name {
	case "active_workloads":
		return float64(len(s.Registry.List())), true
	case "target_count":
		return float64(len(s.Registry.TargetsInUse())), true
	default:
		return 0, false
	}
}
