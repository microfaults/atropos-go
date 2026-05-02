package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/memory"
)

func main() {
	load := flag.Float64("load", 0.5, "target memory load as a fraction (0.0–1.0]")
	duration := flag.Duration("duration", 5*time.Second, "total fault duration (including ramp-up/down)")
	rampUp := flag.Duration("rampup", 1*time.Second, "ramp-up period (0 = instant)")
	rampDown := flag.Duration("rampdown", 1*time.Second, "ramp-down period (0 = instant)")
	chunkSize := flag.Int("chunk", 1<<20, "allocation chunk size in bytes (default 1 MiB)")
	thrashing := flag.Bool("thrashing", false, "enable page-thrashing mode (TLB stress)")
	thrashWorkers := flag.Int("thrashworkers", 2, "number of thrashing goroutines")
	flag.Parse()

	available := memory.AvailableMemory()
	currentUsage := memory.CurrentUsage()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║     atropos-go · Memory Hogger POC       ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Available memory:       %d bytes (%.1f MiB)\n", available, float64(available)/(1024*1024))
	fmt.Printf("  Current usage:          %d bytes (%.1f MiB)\n", currentUsage, float64(currentUsage)/(1024*1024))
	fmt.Printf("  Target load:            %.0f%% of %d bytes\n", *load*100, available)
	targetTotal := uint64(*load * float64(available))
	var targetAlloc uint64
	if targetTotal > currentUsage {
		targetAlloc = targetTotal - currentUsage
	}
	fmt.Printf("  Target allocation:      %d bytes (%.1f MiB)\n", targetAlloc, float64(targetAlloc)/(1024*1024))
	fmt.Printf("  Chunk size:             %d bytes (%.1f KiB)\n", *chunkSize, float64(*chunkSize)/1024)
	fmt.Printf("  Duration:               %s\n", *duration)
	fmt.Printf("  Ramp-up:                %s\n", *rampUp)
	fmt.Printf("  Ramp-down:              %s\n", *rampDown)
	fmt.Printf("  Thrashing:              %v\n", *thrashing)
	if *thrashing {
		fmt.Printf("  Thrash workers:         %d\n", *thrashWorkers)
	}
	fmt.Println()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &memory.Stress{
		Config: memory.Config{
			FaultConfig: fault.FaultConfig{
				Duration: *duration,
				RampUp:   *rampUp,
				RampDown: *rampDown,
			},
			TargetLoad:    *load,
			ChunkSize:     *chunkSize,
			Thrashing:     *thrashing,
			ThrashWorkers: *thrashWorkers,
		},
	}

	handle, err := s.Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Memory hogger started (non-blocking) — doing other work…")
	fmt.Println()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	tick := 1

	for {
		select {
		case result := <-handle.Done():
			fmt.Println()
			fmt.Println("── Memory Hogger Results ────────────────────")
			if d, ok := result.Detail.(memory.Detail); ok {
				fmt.Printf("  Available memory:       %d bytes (%.1f MiB)\n", d.AvailableMemory, float64(d.AvailableMemory)/(1024*1024))
				fmt.Printf("  Target allocation:      %d bytes (%.1f MiB)\n", d.TargetBytes, float64(d.TargetBytes)/(1024*1024))
				fmt.Printf("  Peak allocated:         %d bytes (%.1f MiB)\n", d.PeakAllocated, float64(d.PeakAllocated)/(1024*1024))
				fmt.Printf("  Chunks allocated:       %d\n", d.ChunksAllocated)
				fmt.Printf("  Thrashing enabled:      %v\n", d.ThrashingEnabled)
			}
			fmt.Printf("  Actual duration:        %s\n", result.ActualDuration)
			if result.Err != nil {
				fmt.Printf("  Error:                  %v\n", result.Err)
			} else {
				fmt.Println("  Status:                 completed normally")
			}
			fmt.Println()
			fmt.Println("Tip: run 'docker stats' or 'htop' to see memory usage in real time.")
			return

		case <-ticker.C:
			fmt.Printf("  [main] still running… tick #%d\n", tick)
			tick++
		}
	}
}
