package disk

import (
	"fmt"
	"os"
	"time"

	"atropos-go/internal/fault"
)

const (
	DefaultWriteRate = 10 * 1024 * 1024 // 10 MB/s
	DefaultMaxDisk   = 512 * 1024 * 1024 // 512 MB
	DefaultChunkSize = 1 * 1024 * 1024   // 1 MB per write op

	RefillInterval = 10 * time.Millisecond
)

type Config struct {
	fault.FaultConfig

	// WriteRate is the target write throughput in bytes/sec.
	WriteRate int64

	// MaxDiskUsage is the maximum bytes to occupy on disk.
	// The file grows by appending until this cap, then overwrites in place.
	MaxDiskUsage int64

	// ChunkSize is bytes per write op. Defaults to DefaultChunkSize.
	// Set smaller than WriteRate×duration for low-rate scenarios.
	ChunkSize int64

	// Path is the base directory for the temp file. Defaults to os.TempDir().
	Path string
}

func (c *Config) Validate() error {
	if err := c.FaultConfig.Validate(); err != nil {
		return err
	}
	if c.WriteRate <= 0 {
		return fmt.Errorf("disk: write_rate must be > 0, got %d", c.WriteRate)
	}
	if c.MaxDiskUsage <= 0 {
		return fmt.Errorf("disk: max_disk_usage must be > 0, got %d", c.MaxDiskUsage)
	}
	return nil
}

func (c *Config) EffectiveWriteRate() int64 {
	if c.WriteRate <= 0 {
		return DefaultWriteRate
	}
	return c.WriteRate
}

func (c *Config) EffectiveMaxDiskUsage() int64 {
	if c.MaxDiskUsage <= 0 {
		return DefaultMaxDisk
	}
	return c.MaxDiskUsage
}

func (c *Config) EffectiveChunkSize() int64 {
	if c.ChunkSize <= 0 {
		return DefaultChunkSize
	}
	return c.ChunkSize
}

func (c *Config) EffectivePath() string {
	if c.Path == "" {
		return os.TempDir()
	}
	return c.Path
}
