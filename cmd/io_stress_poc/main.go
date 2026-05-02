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
	iostress "git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/io"
)

func main() {
	readRate := flag.Int64("readrate", 102400, "target read throughput in bytes/sec")
	fileSize := flag.Int("filesize", 4096, "bytes per temp file")
	fileCount := flag.Int("filecount", 100, "number of temp files to create")
	workers := flag.Int("workers", 4, "concurrent read goroutines")
	duration := flag.Duration("duration", 5*time.Second, "total fault duration (Phase 2)")
	rampUp := flag.Duration("rampup", 0, "ramp-up period (0 = instant)")
	rampDown := flag.Duration("rampdown", 0, "ramp-down period (0 = instant)")
	path := flag.String("path", "", "base directory for temp files (default: os.TempDir())")
	flag.Parse()

	fmt.Println("╔══════════════════════════════════════════╗")
	fmt.Println("║     atropos-go · I/O Stress POC         ║")
	fmt.Println("╚══════════════════════════════════════════╝")
	fmt.Println()
	fmt.Printf("  Read rate:              %d bytes/sec (%.1f KB/s)\n", *readRate, float64(*readRate)/1024)
	fmt.Printf("  File size:              %d bytes\n", *fileSize)
	fmt.Printf("  File count:             %d\n", *fileCount)
	fmt.Printf("  Workers:                %d\n", *workers)
	fmt.Printf("  Duration (Phase 2):     %s\n", *duration)
	fmt.Printf("  Ramp-up:                %s\n", *rampUp)
	fmt.Printf("  Ramp-down:              %s\n", *rampDown)
	if *path != "" {
		fmt.Printf("  Path:                   %s\n", *path)
	} else {
		fmt.Printf("  Path:                   %s (default)\n", os.TempDir())
	}
	fmt.Println()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	s := &iostress.Stress{
		Config: iostress.Config{
			FaultConfig: fault.FaultConfig{
				Duration: *duration,
				RampUp:   *rampUp,
				RampDown: *rampDown,
			},
			ReadRate:  *readRate,
			FileSize:  *fileSize,
			FileCount: *fileCount,
			Workers:   *workers,
			Path:      *path,
		},
	}

	fmt.Printf("Phase 1: writing %d × %d-byte files…\n", *fileCount, *fileSize)
	handle, err := s.Start(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Config error: %v\n", err)
		os.Exit(1)
	}
	totalWritten := int64(*fileCount) * int64(*fileSize)
	fmt.Printf("Phase 1: done — wrote %d bytes (%.1f KB)\n", totalWritten, float64(totalWritten)/1024)
	fmt.Println()
	fmt.Printf("Phase 2: reading at %d bytes/sec — press Ctrl+C to stop early…\n", *readRate)
	fmt.Println()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	tick := 1

	for {
		select {
		case result := <-handle.Done():
			fmt.Println()
			fmt.Println("── I/O Stress Results ───────────────────────")
			if d, ok := result.Detail.(iostress.Detail); ok {
				fmt.Printf("  Files created:          %d × %d bytes\n", d.FileCount, d.FileSize)
				fmt.Printf("  Total bytes written:    %d (%.1f KB)\n", d.TotalBytesWritten, float64(d.TotalBytesWritten)/1024)
				fmt.Printf("  Total bytes read:       %d (%.1f KB)\n", d.TotalBytesRead, float64(d.TotalBytesRead)/1024)
				fmt.Printf("  Total read ops:         %d\n", d.TotalReadOps)
				fmt.Printf("  Effective read rate:    %.1f KB/s\n", float64(d.TotalBytesRead)/result.ActualDuration.Seconds()/1024)
			}
			fmt.Printf("  Actual duration:        %s\n", result.ActualDuration)
			if result.Err != nil {
				fmt.Printf("  Error:                  %v\n", result.Err)
			} else {
				fmt.Println("  Status:                 completed normally")
			}
			fmt.Println()
			fmt.Println("Tip: run 'docker stats' to see I/O usage in real time.")
			return

		case <-ticker.C:
			fmt.Printf("  [main] reading… tick #%d\n", tick)
			tick++
		}
	}
}
