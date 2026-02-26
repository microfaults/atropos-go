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

	"github.com/pronei/faults-lab/fault"
)

func main() {
	cores := flag.Int("cores", 1, "number of CPU cores to saturate")
	duration := flag.Duration("duration", 5*time.Second, "how long to spike CPU")
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║       atropos-go · CPU Spike POC         ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  System cores (NumCPU):  %d\n", runtime.NumCPU())
	fmt.Printf("  GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  Requested cores:        %d\n", *cores)
	fmt.Printf("  Requested duration:     %s\n", *duration)
	fmt.Println()

	// Allow Ctrl+C to cancel early.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := fault.CPUSpikeConfig{
		Cores:    *cores,
		Duration: *duration,
	}

	fmt.Println("🔥 Starting CPU spike…")
	start := time.Now()
	result := fault.InjectCPUSpike(ctx, cfg)
	wall := time.Since(start)

	fmt.Println()
	fmt.Println("── Results ─────────────────────────────────")
	fmt.Printf("  Actual cores used:      %d\n", result.ActualCores)
	fmt.Printf("  Actual duration:        %s\n", result.ActualDuration)
	fmt.Printf("  Wall-clock elapsed:     %s\n", wall)
	if result.Err != nil {
		fmt.Printf("  Error:                  %v\n", result.Err)
	} else {
		fmt.Println("  Status:                 ✅ completed normally")
	}
	fmt.Println()

	// Tip for the user.
	fmt.Println("💡 Tip: run 'docker stats' in another terminal to see CPU usage in real time.")
}
