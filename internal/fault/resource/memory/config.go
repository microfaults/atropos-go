package memory

import (
	"fmt"

	"github.com/microfaults/atropos-go/internal/fault"
)

// Default config values.
const (
	DefaultChunkSize    = 1 << 20 // 1 MiB per allocation chunk
	DefaultThrashWorkers = 2
)

// Config holds parameters for a memory-pressure fault.
//
// The two-phase model:
//   - Phase 1 (Allocate/Ramp): gradually allocate byte-slice chunks until
//     TargetLoad × AvailableMemory bytes are consumed. Each chunk is
//     page-touched to commit real RSS.
//   - Phase 2 (Hold + optional Thrash): hold allocations for the remaining
//     duration. If Thrashing is enabled, worker goroutines continuously
//     walk the allocated pages in a TLB-hostile stride pattern.
//
// TargetLoad is the primary tuning knob.
type Config struct {
	fault.FaultConfig

	// TargetLoad is the fraction of available memory to consume (0.0, 1.0].
	// For example, 0.7 means consume 70% of the cgroup (or system) memory limit.
	TargetLoad float64

	// ChunkSize is the size of each allocation chunk in bytes.
	// Smaller chunks give finer granularity during ramp phases but add
	// slice overhead. Defaults to 1 MiB if zero.
	ChunkSize int

	// Thrashing enables page-thrashing mode. When true, allocated memory
	// is continuously accessed in a stride pattern that stresses the TLB
	// and page tables, simulating real-world memory pressure from working
	// set churn.
	Thrashing bool

	// ThrashWorkers is the number of goroutines that concurrently access
	// allocated memory when Thrashing is enabled. Defaults to 2 if zero.
	ThrashWorkers int
}

// Validate checks that the memory config is valid.
func (c *Config) Validate() error {
	if err := c.FaultConfig.Validate(); err != nil {
		return err
	}
	if c.TargetLoad <= 0 || c.TargetLoad > 1.0 {
		return fmt.Errorf("memory: target_load must be in (0.0, 1.0], got %.2f", c.TargetLoad)
	}
	if c.ChunkSize < 0 {
		return fmt.Errorf("memory: chunk_size must be >= 0, got %d", c.ChunkSize)
	}
	if c.Thrashing && c.ThrashWorkers < 0 {
		return fmt.Errorf("memory: thrash_workers must be >= 0 when thrashing is enabled, got %d", c.ThrashWorkers)
	}
	return nil
}

// EffectiveChunkSize returns ChunkSize, applying the default if zero.
func (c *Config) EffectiveChunkSize() int {
	if c.ChunkSize <= 0 {
		return DefaultChunkSize
	}
	return c.ChunkSize
}

// EffectiveThrashWorkers returns ThrashWorkers, applying the default if zero.
func (c *Config) EffectiveThrashWorkers() int {
	if c.ThrashWorkers <= 0 {
		return DefaultThrashWorkers
	}
	return c.ThrashWorkers
}
