// Package faultparams is the single source of truth for the per-(category,
// fault_type) parameter schemas that the atropos SDK decodes. The structs here
// are used verbatim by the SDK's own decoders (compiled_rule.go), so the wire
// contract and this package cannot drift. Control planes (manteion) import
// this package to validate fault params before persisting or pushing them.
//
// Durations are Go duration strings ("250ms", "1.5s"); byte sizes and rates
// are plain integers. Zero values mean "use the SDK default" where one is
// documented, otherwise the field is required.
//
// The package is dependency-free by design (stdlib only) so control planes
// can import it without pulling in the SDK runtime.
package faultparams

import (
	"errors"
	"fmt"
	"time"
)

// Param is one fault type's parameter set. Implementations are plain structs
// whose json tags define the wire shape.
type Param interface {
	// Validate checks field-level constraints. It mirrors (and may be
	// stricter than) what the SDK decoder accepts: anything that fails
	// decode also fails Validate, plus range checks the SDK only catches
	// at fault start.
	Validate() error
}

// ---------------------------------------------------------------------------
// inline
// ---------------------------------------------------------------------------

// InlineLatency delays request handling in-process.
// Effective delay per request = delay + rand(jitter).
type InlineLatency struct {
	Delay  string `json:"delay"`            // required, e.g. "250ms"
	Jitter string `json:"jitter,omitempty"` // optional uniform jitter, e.g. "50ms"
}

func (p InlineLatency) Validate() error {
	if _, err := time.ParseDuration(p.Delay); err != nil {
		return fmt.Errorf("inline latency: invalid delay %q: %w", p.Delay, err)
	}
	if p.Jitter != "" {
		if _, err := time.ParseDuration(p.Jitter); err != nil {
			return fmt.Errorf("inline latency: invalid jitter %q: %w", p.Jitter, err)
		}
	}
	return nil
}

// InlineError short-circuits the request with an HTTP error response.
// The SDK defaults status_code 0 → 500 and an empty message → "injected fault".
type InlineError struct {
	StatusCode int    `json:"status_code,omitempty"` // 100..599; 0 = SDK default (500)
	Message    string `json:"message,omitempty"`     // response body; "" = SDK default
}

func (p InlineError) Validate() error {
	if p.StatusCode != 0 && (p.StatusCode < 100 || p.StatusCode > 599) {
		return fmt.Errorf("inline error: status_code %d outside 100..599", p.StatusCode)
	}
	return nil
}

// InlineHang blocks the request until the duration elapses or the client
// gives up — an HTTP-level blackhole.
type InlineHang struct {
	Duration string `json:"duration"` // required, e.g. "30s"
}

func (p InlineHang) Validate() error {
	if _, err := time.ParseDuration(p.Duration); err != nil {
		return fmt.Errorf("inline hang: invalid duration %q: %w", p.Duration, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// resource
// ---------------------------------------------------------------------------

// ResourceCPU burns CPU at an incremental target load with a duty-cycle window.
type ResourceCPU struct {
	TargetLoad float64 `json:"target_load"`      // required, fraction in (0,1]
	Window     string  `json:"window,omitempty"` // duty-cycle period; "" = SDK default (100ms)
}

func (p ResourceCPU) Validate() error {
	if p.TargetLoad <= 0 || p.TargetLoad > 1 {
		return fmt.Errorf("resource cpu: target_load %v outside (0,1]", p.TargetLoad)
	}
	if p.Window != "" {
		if _, err := time.ParseDuration(p.Window); err != nil {
			return fmt.Errorf("resource cpu: invalid window %q: %w", p.Window, err)
		}
	}
	return nil
}

// ResourceMemory allocates (and optionally thrashes) memory toward a target
// fraction of available memory.
type ResourceMemory struct {
	TargetLoad    float64 `json:"target_load"`              // required, fraction in (0,1]
	ChunkSize     int     `json:"chunk_size,omitempty"`     // bytes per chunk; 0 = SDK default (1 MiB)
	Thrashing     bool    `json:"thrashing,omitempty"`      // page-thrash mode
	ThrashWorkers int     `json:"thrash_workers,omitempty"` // 0 = SDK default (2)
}

func (p ResourceMemory) Validate() error {
	if p.TargetLoad <= 0 || p.TargetLoad > 1 {
		return fmt.Errorf("resource memory: target_load %v outside (0,1]", p.TargetLoad)
	}
	if p.ChunkSize < 0 {
		return errors.New("resource memory: chunk_size must be >= 0")
	}
	if p.ThrashWorkers < 0 {
		return errors.New("resource memory: thrash_workers must be >= 0")
	}
	return nil
}

// ResourceDisk fills disk at a sustained write rate up to a usage cap.
type ResourceDisk struct {
	WriteRate    int64  `json:"write_rate,omitempty"`     // bytes/sec; 0 = SDK default (10 MB/s)
	MaxDiskUsage int64  `json:"max_disk_usage,omitempty"` // bytes; 0 = SDK default (512 MB)
	ChunkSize    int64  `json:"chunk_size,omitempty"`     // bytes; 0 = SDK default (1 MB)
	Path         string `json:"path,omitempty"`           // scratch dir; "" = os.TempDir()
}

func (p ResourceDisk) Validate() error {
	if p.WriteRate < 0 || p.MaxDiskUsage < 0 || p.ChunkSize < 0 {
		return errors.New("resource disk: rates and sizes must be >= 0")
	}
	return nil
}

// ResourceIO generates filesystem read/write pressure across a file set.
type ResourceIO struct {
	ReadRate  int64  `json:"read_rate,omitempty"`  // bytes/sec; 0 = SDK default (100 KB/s)
	FileSize  int    `json:"file_size,omitempty"`  // bytes; 0 = SDK default (4096)
	FileCount int    `json:"file_count,omitempty"` // 0 = SDK default (256)
	Workers   int    `json:"workers,omitempty"`    // 0 = SDK default (4)
	Path      string `json:"path,omitempty"`       // scratch dir; "" = os.TempDir()
	Mode      string `json:"mode,omitempty"`       // "read" (default) | "write" | "read_write"
}

func (p ResourceIO) Validate() error {
	if p.ReadRate < 0 || p.FileSize < 0 || p.FileCount < 0 || p.Workers < 0 {
		return errors.New("resource io: rates, sizes and counts must be >= 0")
	}
	switch p.Mode {
	case "", "read", "write", "read_write":
		return nil
	default:
		return fmt.Errorf("resource io: unknown mode %q (want read|write|read_write)", p.Mode)
	}
}

// ---------------------------------------------------------------------------
// network (toxics — run inside the TCP proxy; the envelope lives outside)
// ---------------------------------------------------------------------------

// NetworkLatency delays piped bytes by delay + rand(jitter).
type NetworkLatency struct {
	Delay  string `json:"delay"`            // required, e.g. "100ms"
	Jitter string `json:"jitter,omitempty"` // optional uniform jitter
}

func (p NetworkLatency) Validate() error {
	if _, err := time.ParseDuration(p.Delay); err != nil {
		return fmt.Errorf("network latency: invalid delay %q: %w", p.Delay, err)
	}
	if p.Jitter != "" {
		if _, err := time.ParseDuration(p.Jitter); err != nil {
			return fmt.Errorf("network latency: invalid jitter %q: %w", p.Jitter, err)
		}
	}
	return nil
}

// NetworkRetransmitDelay simulates TCP retransmission stalls: a fraction of
// reads is delayed, optionally RSTing after a threshold of stalls.
type NetworkRetransmitDelay struct {
	Rate           float64 `json:"rate"`                      // required, fraction in [0,1]
	Delay          string  `json:"delay,omitempty"`           // per-stall delay; "" = SDK default (200ms)
	ResetThreshold int     `json:"reset_threshold,omitempty"` // stalls before RST; 0 = never
}

func (p NetworkRetransmitDelay) Validate() error {
	if p.Rate < 0 || p.Rate > 1 {
		return fmt.Errorf("network retransmit_delay: rate %v outside [0,1]", p.Rate)
	}
	if p.Delay != "" {
		if _, err := time.ParseDuration(p.Delay); err != nil {
			return fmt.Errorf("network retransmit_delay: invalid delay %q: %w", p.Delay, err)
		}
	}
	if p.ResetThreshold < 0 {
		return errors.New("network retransmit_delay: reset_threshold must be >= 0")
	}
	return nil
}

// NetworkBlackhole consumes client bytes and never responds.
type NetworkBlackhole struct{}

func (p NetworkBlackhole) Validate() error { return nil }

// NetworkDrip trickles the stream out in tiny chunks.
type NetworkDrip struct {
	ChunkSize int    `json:"chunk_size,omitempty"` // bytes per chunk; 0 = SDK default (1)
	Interval  string `json:"interval,omitempty"`   // pause between chunks
}

func (p NetworkDrip) Validate() error {
	if p.ChunkSize < 0 {
		return errors.New("network drip: chunk_size must be >= 0")
	}
	if p.Interval != "" {
		if _, err := time.ParseDuration(p.Interval); err != nil {
			return fmt.Errorf("network drip: invalid interval %q: %w", p.Interval, err)
		}
	}
	return nil
}

// NetworkRST force-closes the connection with a TCP RST after a byte count
// and/or duration. Zero values for both mean an immediate RST.
type NetworkRST struct {
	AfterBytes    int64  `json:"after_bytes,omitempty"`    // forwarded bytes before RST
	AfterDuration string `json:"after_duration,omitempty"` // elapsed time before RST
}

func (p NetworkRST) Validate() error {
	if p.AfterBytes < 0 {
		return errors.New("network rst: after_bytes must be >= 0")
	}
	if p.AfterDuration != "" {
		if _, err := time.ParseDuration(p.AfterDuration); err != nil {
			return fmt.Errorf("network rst: invalid after_duration %q: %w", p.AfterDuration, err)
		}
	}
	return nil
}

// NetworkThrottle caps stream bandwidth with token pacing.
type NetworkThrottle struct {
	BytesPerSec int64 `json:"bytes_per_sec"` // required, > 0
}

func (p NetworkThrottle) Validate() error {
	if p.BytesPerSec <= 0 {
		return errors.New("network throttle: bytes_per_sec must be > 0")
	}
	return nil
}
