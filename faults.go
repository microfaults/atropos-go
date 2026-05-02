// faults.go
package atropos

import (
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/cpu"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/disk"
	iostress "git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/io"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/memory"
)

// NewLatencyFault creates a fault that delays by base + rand(jitter).
func NewLatencyFault(delay, jitter time.Duration) Fault {
	return &inline.Latency{
		FaultConfig: fault.FaultConfig{Duration: delay + jitter},
		Delay:       delay,
		Jitter:      jitter,
	}
}

// NewHangFault creates a fault that blocks until duration expires.
func NewHangFault(duration time.Duration) Fault {
	return &inline.Hang{
		FaultConfig: fault.FaultConfig{Duration: duration},
	}
}

// NewErrorFault creates a fault that completes immediately with an HTTP error code.
func NewErrorFault(statusCode int, message string) Fault {
	return &inline.Error{
		FaultConfig: fault.FaultConfig{Duration: 1}, // instant; must be >0 for validation
		StatusCode:  statusCode,
		Message:     message,
	}
}

func NewDiskStressFault(duration time.Duration, writeRate, maxDiskUsage, chunkSize int64, path string) Fault {
	return &disk.Stress{
		Config: disk.Config{
			FaultConfig:  fault.FaultConfig{Duration: duration},
			WriteRate:    writeRate,
			MaxDiskUsage: maxDiskUsage,
			ChunkSize:    chunkSize,
			Path:         path,
		},
	}
}

func NewCPUStressFault(duration time.Duration, targetLoad float64) Fault {
	return &cpu.Stress{Config: resource.Config{
		FaultConfig: fault.FaultConfig{Duration: duration},
		TargetLoad:  targetLoad,
	}}
}

func NewMemoryStressFault(duration time.Duration, targetLoad float64, thrashing bool) Fault {
	return &memory.Stress{Config: memory.Config{
		FaultConfig: fault.FaultConfig{Duration: duration},
		TargetLoad:  targetLoad,
		Thrashing:   thrashing,
	}}
}

func NewIOStressFault(duration time.Duration, readRate int64) Fault {
	return &iostress.Stress{Config: iostress.Config{
		FaultConfig: fault.FaultConfig{Duration: duration},
		ReadRate:    readRate,
		FileSize:    iostress.DefaultFileSize,
		FileCount:   iostress.DefaultFileCount,
		Workers:     iostress.DefaultWorkers,
	}}
}
