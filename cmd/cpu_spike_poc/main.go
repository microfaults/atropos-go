package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/fault/resource"
	"github.com/microfaults/atropos-go/internal/fault/resource/cpu"
)

func main() {
	load := flag.Float64("load", 0.5, "target total CPU load as a fraction (0.0–1.0]")
	duration := flag.Duration("duration", 5*time.Second, "total fault duration (including ramp-up)")
	rampUp := flag.Duration("rampup", 1*time.Second, "ramp-up period (0 = instant)")
	rampDown := flag.Duration("rampdown", 1*time.Second, "ramp-down period (0 = instant)")
	flag.Parse()

	available := cpu.AvailableCPUs()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║     atropos-go · CPU Throttle POC        ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Host cores (NumCPU):    %d\n", runtime.NumCPU())
	fmt.Printf("  Available CPUs (cgroup): %.2f\n", available)
	fmt.Printf("  GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  Target load (total):    %.0f%% of %.2f CPUs\n", *load*100, available)
	fmt.Printf("  Duration:               %s\n", *duration)
	fmt.Printf("  Ramp-up:                %s\n", *rampUp)
	fmt.Println()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &cpu.Stress{
		Config: resource.Config{
			FaultConfig: fault.FaultConfig{
				Duration: *duration,
				RampUp:   *rampUp,
				RampDown: *rampDown,
			},
			TargetLoad: *load,
		},
	}

	handle, err := s.Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("CPU throttle started (non-blocking) — doing other work…")
	fmt.Println()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	tick := 1

	for {
		select {
		case result := <-handle.Done():
			fmt.Println()
			fmt.Println("── Throttle Results ─────────────────────────")
			if d, ok := result.Detail.(cpu.Detail); ok {
				fmt.Printf("  Available CPUs:         %.2f\n", d.AvailableCPUs)
				fmt.Printf("  Workers spawned:        %d\n", d.Workers)
				fmt.Printf("  Per-worker load:        %.0f%%\n", d.PerWorkerLoad*100)
			}
			fmt.Printf("  Actual duration:        %s\n", result.ActualDuration)
			if result.Err != nil {
				fmt.Printf("  Error:                  %v\n", result.Err)
			} else {
				fmt.Println("  Status:                 completed normally")
			}
			fmt.Println()
			fmt.Println("Tip: run 'docker stats' or Task Manager to see CPU usage in real time.")
			return

		case <-ticker.C:
			fmt.Printf("  [main] still running… tick #%d\n", tick)
			tick++
		}
	}
}
