// faults.go
package atropos

import (
	"time"

	"atropos-go/internal/fault"
	"atropos-go/internal/fault/inline"
	"atropos-go/internal/fault/resource/disk"
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