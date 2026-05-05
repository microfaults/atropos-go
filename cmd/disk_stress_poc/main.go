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
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/disk"
)

func main() {
	writeRate := flag.Int64("writerate", 10*1024*1024, "target write bandwidth in bytes/sec (default 10 MB/s)")
	maxDisk := flag.Int64("maxdisk", 512*1024*1024, "max bytes to occupy on disk before overwriting (default 512 MB)")
	chunkSize := flag.Int64("chunksize", 1*1024*1024, "bytes per write op (default 1 MB)")
	duration := flag.Duration("duration", 5*time.Second, "total fault duration")
	rampUp := flag.Duration("rampup", 0, "ramp-up period (0 = instant)")
	rampDown := flag.Duration("rampdown", 0, "ramp-down period (0 = instant)")
	path := flag.String("path", "", "directory for temp file (default: os.TempDir())")
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║     atropos-go · Disk Stress POC         ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Write rate:    %d bytes/s (%.1f MB/s)\n", *writeRate, float64(*writeRate)/1024/1024)
	fmt.Printf("  Max disk:      %d bytes (%.1f MB)\n", *maxDisk, float64(*maxDisk)/1024/1024)
	fmt.Printf("  Chunk size:    %d bytes\n", *chunkSize)
	fmt.Printf("  Duration:      %s\n", *duration)
	fmt.Printf("  Ramp-up:       %s\n", *rampUp)
	fmt.Printf("  Ramp-down:     %s\n", *rampDown)
	fmt.Println()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &disk.Stress{
		Config: disk.Config{
			FaultConfig: fault.FaultConfig{
				Duration: *duration,
				RampUp:   *rampUp,
				RampDown: *rampDown,
			},
			WriteRate:    *writeRate,
			MaxDiskUsage: *maxDisk,
			ChunkSize:    *chunkSize,
			Path:         *path,
		},
	}

	handle, err := s.Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Disk stress started (non-blocking) — doing other work…")
	fmt.Println()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	tick := 1

	for {
		select {
		case result := <-handle.Done():
			fmt.Println()
			fmt.Println("── Disk Stress Results ──────────────────────")
			if d, ok := result.Detail.(disk.Detail); ok {
				fmt.Printf("  Total written:  %d bytes (%.1f MB)\n", d.TotalBytesWritten, float64(d.TotalBytesWritten)/1024/1024)
				fmt.Printf("  Write ops:      %d\n", d.TotalWriteOps)
				fmt.Printf("  Max disk cap:   %d bytes (%.1f MB)\n", d.MaxDiskUsage, float64(d.MaxDiskUsage)/1024/1024)
			}
			fmt.Printf("  Actual duration: %s\n", result.ActualDuration)
			if result.Err != nil {
				fmt.Printf("  Error:           %v\n", result.Err)
			} else {
				fmt.Println("  Status:          completed normally")
			}
			fmt.Println()
			fmt.Println("Tip: run 'docker stats' or 'kubectl top pod' to see disk I/O in real time.")
			return

		case <-ticker.C:
			fmt.Printf("  [main] still running… tick #%d\n", tick)
			tick++
		}
	}
}
