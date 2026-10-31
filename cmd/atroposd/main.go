// Command atroposd is the atropos sidecar: the out-of-process host of the
// instrument for services that cannot embed the Go SDK (or choose not to).
//
//	atroposd [-config /etc/atropos/atroposd.yaml] [-print-config]
//
// Configuration comes from the YAML file (optional) overlaid by the
// environment; see internal/atroposd.Load for the variables. Explicit
// capture mode (M1): the app dials the sidecar's egress ports instead of
// its dependencies, e.g. PRODUCT_CATALOG_SERVICE_ADDR=127.0.0.1:19090 with
// ATROPOSD_EGRESS=":19090=productcatalogservice:3550".
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"git.ucsc.edu/microfaults/atropos-go/internal/atroposd"

	"gopkg.in/yaml.v3"
)

func main() {
	configPath := flag.String("config", defaultConfigPath(), "YAML config file (optional; the environment overrides it)")
	printCfg := flag.Bool("print-config", false, "print the effective configuration and exit")
	flag.Parse()

	// main's own errors only (a config that does not load, the exit error):
	// Run logs through the SDK default logger, stdout JSON teed into OTel.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := atroposd.Load(*configPath)
	if err != nil {
		logger.Error("atroposd: load config", "error", err)
		os.Exit(2)
	}
	if *printCfg {
		if err := yaml.NewEncoder(os.Stdout).Encode(cfg); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := atroposd.Run(ctx, cfg, nil); err != nil {
		logger.Error("atroposd: exit", "error", err)
		os.Exit(1)
	}
}

// defaultConfigPath is ATROPOSD_CONFIG, else /etc/atropos/atroposd.yaml when
// it exists, else none (environment only).
func defaultConfigPath() string {
	if v := os.Getenv("ATROPOSD_CONFIG"); v != "" {
		return v
	}
	if _, err := os.Stat("/etc/atropos/atroposd.yaml"); err == nil {
		return "/etc/atropos/atroposd.yaml"
	}
	return ""
}
