package io

import (
	"fmt"
	"os"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// IOMode selects which I/O direction(s) the fault exercises.
type IOMode int

const (
	// ModeWrite stresses write I/O only (Phase 1 writes, then exits).
	ModeWrite IOMode = iota
	// ModeRead writes files first, then rate-controls reads.
	ModeRead
	// ModeReadWrite writes files first, then rate-controls reads and writes.
	ModeReadWrite
)

// Default config values.
const (
	DefaultFileSize  = 4096 // 4 KB per file
	DefaultFileCount = 256
	DefaultWorkers   = 4
	DefaultReadRate  = 102400 // 100 KB/s
)

// Config holds parameters for an I/O stress fault.
//
// The two-phase model:
//   - Phase 1 (write): create FileCount files of FileSize random bytes — runs
//     as fast as possible, blocks until all files exist.
//   - Phase 2 (read):  read files back at ReadRate bytes/sec using a shared
//     token-bucket throttle across Workers goroutines.
//
// ReadRate is the primary tuning knob.
type Config struct {
	fault.FaultConfig

	// ReadRate is the target read throughput in bytes/sec.
	// The token bucket refills at this rate.
	ReadRate int64

	// FileSize is the number of random bytes per file.
	// Smaller files ⇒ more open/read/close syscalls per byte.
	FileSize int

	// FileCount is the number of files to create in Phase 1.
	FileCount int

	// Workers is the number of concurrent read goroutines in Phase 2.
	Workers int

	// Path is the base directory for temp files. If empty, os.TempDir() is used.
	Path string

	// Mode selects which I/O direction(s) to exercise.
	// Defaults to ModeRead (write → rate-controlled read).
	Mode IOMode
}

// Validate checks that the I/O config is valid.
func (c *Config) Validate() error {
	if err := c.FaultConfig.Validate(); err != nil {
		return err
	}
	if c.ReadRate <= 0 {
		return fmt.Errorf("io: read_rate must be > 0, got %d", c.ReadRate)
	}
	if c.FileSize <= 0 {
		return fmt.Errorf("io: file_size must be > 0, got %d", c.FileSize)
	}
	if c.FileCount <= 0 {
		return fmt.Errorf("io: file_count must be > 0, got %d", c.FileCount)
	}
	if c.Workers <= 0 {
		return fmt.Errorf("io: workers must be > 0, got %d", c.Workers)
	}
	return nil
}

// EffectiveFileSize returns FileSize, applying the default if zero.
func (c *Config) EffectiveFileSize() int {
	if c.FileSize <= 0 {
		return DefaultFileSize
	}
	return c.FileSize
}

// EffectiveFileCount returns FileCount, applying the default if zero.
func (c *Config) EffectiveFileCount() int {
	if c.FileCount <= 0 {
		return DefaultFileCount
	}
	return c.FileCount
}

// EffectiveWorkers returns Workers, applying the default if zero.
func (c *Config) EffectiveWorkers() int {
	if c.Workers <= 0 {
		return DefaultWorkers
	}
	return c.Workers
}

// EffectiveReadRate returns ReadRate, applying the default if zero.
func (c *Config) EffectiveReadRate() int64 {
	if c.ReadRate <= 0 {
		return DefaultReadRate
	}
	return c.ReadRate
}

// EffectivePath returns Path, applying os.TempDir() if empty.
func (c *Config) EffectivePath() string {
	if c.Path == "" {
		return os.TempDir()
	}
	return c.Path
}

// RefillInterval is the token-bucket tick interval.
// The bucket adds (ReadRate * RefillInterval / 1s) tokens per tick.
const RefillInterval = 10 * time.Millisecond
