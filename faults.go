// faults.go
package atropos

import (
	"time"

	"github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/fault/inline"
	"github.com/microfaults/atropos-go/internal/fault/resource"
	"github.com/microfaults/atropos-go/internal/fault/resource/cpu"
	"github.com/microfaults/atropos-go/internal/fault/resource/memory"
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

// CPUStressOpts configures a CPU stress fault.
type CPUStressOpts struct {
	Duration time.Duration // total duration including ramps
	Load     float64       // fraction of CPU to consume (0,1]
	RampUp   time.Duration // linear ramp 0→target
	RampDown time.Duration // linear ramp target→0
}

// NewCPUStressFault creates a background CPU stress fault.
func NewCPUStressFault(opts CPUStressOpts) *cpu.Stress {
	return &cpu.Stress{
		Config: resource.Config{
			FaultConfig: fault.FaultConfig{
				Duration: opts.Duration,
				RampUp:   opts.RampUp,
				RampDown: opts.RampDown,
			},
			TargetLoad: opts.Load,
		},
	}
}

// MemoryStressOpts configures a memory stress fault.
type MemoryStressOpts struct {
	Duration  time.Duration // total duration including ramps
	Load      float64       // fraction of memory to consume (0,1]
	RampUp    time.Duration // linear ramp 0→target
	RampDown  time.Duration // linear ramp target→0
	Thrashing bool          // enable TLB-hostile page thrashing
}

// NewMemoryStressFault creates a background memory stress fault.
func NewMemoryStressFault(opts MemoryStressOpts) *memory.Stress {
	return &memory.Stress{
		Config: memory.Config{
			FaultConfig: fault.FaultConfig{
				Duration: opts.Duration,
				RampUp:   opts.RampUp,
				RampDown: opts.RampDown,
			},
			TargetLoad: opts.Load,
			Thrashing:  opts.Thrashing,
		},
	}
}
