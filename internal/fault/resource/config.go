package resource

import (
	"fmt"
	"time"

	fault "git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// Config holds parameters shared by all resource-pressure faults
// (CPU, memory, disk I/O). These faults consume a tunable fraction
// of a system resource over a bounded duration.
//
// Inspired by the iBench SoI model: every resource fault has a tunable
// intensity [0,1] whose impact scales linearly with the target load,
// plus a duty-cycle window for load shaping.
type Config struct {
	fault.FaultConfig

	// TargetLoad is the INCREMENTAL fraction of the resource to add on top
	// of baseline (0.0, 1.0]. Setting 0.10 adds 10% × capacity of new load;
	// it does NOT drive total utilisation to 10%.
	//
	// Consumed by:
	//   - CPU:    spawns ⌈TargetLoad × cores⌉ workers, each duty-cycling
	//             at TargetLoad of every Window
	//   - Memory: allocates TargetLoad × AvailableMemory bytes, capped only
	//             for OOM safety (never reduced to "match" baseline)
	//
	// Ignored by IO and Disk, which use explicit rate knobs (ReadRate /
	// WriteRate in bytes/sec). See ambiguities.md A15 for the structural
	// follow-up.
	//
	// Intensity ramps linearly during RampUp/RampDown phases.
	TargetLoad float64

	// Window is the duty-cycle period for load shaping.
	// Within each window the fault is active for (load × window) and
	// idle for the remainder. Smaller windows give finer control.
	// Defaults to 100ms if zero.
	//
	// Used by CPU. Memory has no duty cycle (allocations are sticky);
	// IO and Disk shape via token-bucket refill rate, not Window.
	Window time.Duration
}

const DefaultWindow = 100 * time.Millisecond

// Validate checks that the resource config is valid.
func (c *Config) Validate() error {
	if err := c.FaultConfig.Validate(); err != nil {
		return err
	}
	if c.TargetLoad <= 0 || c.TargetLoad > 1.0 {
		return fmt.Errorf("resource: target_load must be in (0.0, 1.0], got %.2f", c.TargetLoad)
	}
	return nil
}

// EffectiveWindow returns the duty-cycle window, applying the default if zero.
func (c *Config) EffectiveWindow() time.Duration {
	if c.Window <= 0 {
		return DefaultWindow
	}
	return c.Window
}
