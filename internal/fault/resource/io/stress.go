package io

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microfaults/atropos-go/internal/fault"
	"github.com/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Stress is an I/O-pressure fault that creates many small random-hex files
// and then reads them back at a controlled rate.
//
// Phase 1 (write): dumps FileCount files of FileSize random bytes into a
// temporary directory. Blocks until all writes are complete.
//
// Phase 2 (read): spawns Workers goroutines that each pick files round-robin
// and read them at a combined rate of ReadRate bytes/sec, throttled by a
// shared token bucket.
//
// Implements fault.Fault and fault.EventAware.
type Stress struct {
	Config
	emit fault.EventEmitter
}

// Detail carries I/O-specific diagnostics from a completed fault.
type Detail struct {
	TotalBytesWritten int64
	TotalBytesRead    int64
	TotalWriteOps     int64
	TotalReadOps      int64
	FileCount         int
	FileSize          int
	TempDir           string
}

// SetEventEmitter implements fault.EventAware.
func (s *Stress) SetEventEmitter(fn fault.EventEmitter) {
	s.emit = fn
}

// emitEvent is a nil-safe helper.
func (s *Stress) emitEvent(name string, attrs ...attribute.KeyValue) {
	if s.emit != nil {
		s.emit(name, attrs...)
	}
}

// Validate checks that the I/O stress config is valid.
func (s *Stress) Validate() error {
	return s.Config.Validate()
}

// Start begins the I/O stress fault.
//
// Phase 1 runs synchronously (blocks until all files are written).
// Phase 2 runs in the background — the returned Handle lets the caller
// wait for completion or request early cancellation.
func (s *Stress) Start(ctx context.Context) (*fault.Handle, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	fileSize := s.EffectiveFileSize()
	fileCount := s.EffectiveFileCount()
	workers := s.EffectiveWorkers()
	readRate := s.EffectiveReadRate()
	basePath := s.EffectivePath()

	// ── Phase 1: write files ────────────────────────────────────────
	tmpDir, err := os.MkdirTemp(basePath, "atropos-io-")
	if err != nil {
		return nil, fmt.Errorf("io: failed to create temp dir: %w", err)
	}

	files := make([]string, fileCount)
	var totalBytesWritten int64

	for i := 0; i < fileCount; i++ {
		name := filepath.Join(tmpDir, fmt.Sprintf("file_%04d.hex", i))
		if err := writeRandomFile(name, fileSize); err != nil {
			// Best-effort cleanup on failure.
			os.RemoveAll(tmpDir)
			return nil, fmt.Errorf("io: failed to write file %d: %w", i, err)
		}
		files[i] = name
		totalBytesWritten += int64(fileSize)
	}

	// ── Phase 2: rate-controlled reads ──────────────────────────────
	throttleCtx, cancel := context.WithTimeout(ctx, s.Duration)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()
		defer os.RemoveAll(tmpDir) // cleanup temp files

		var (
			totalRead atomic.Int64
			totalOps  atomic.Int64
			wg        sync.WaitGroup
		)

		bucket := newTokenBucket(readRate, int64(fileSize))
		start := time.Now()

		// Ramp controller: adjusts bucket refill rate over time.
		rampDone := make(chan struct{})
		go func() {
			defer close(rampDone)
			rampLoop(throttleCtx, bucket, readRate, s.Duration, s.RampUp, s.RampDown, start)
		}()

		// Phase-tracking goroutine: emits ramp events at transitions.
		if s.emit != nil && (s.RampUp > 0 || s.RampDown > 0) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.emitPhaseEvents(throttleCtx, start, readRate)
			}()
		}

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				readWorker(throttleCtx, bucket, files, fileSize, workerID, &totalRead, &totalOps)
			}(i)
		}

		wg.Wait()
		<-rampDone
		elapsed := time.Since(start)

		result := fault.Result{
			ActualDuration: elapsed,
			Detail: Detail{
				TotalBytesWritten: totalBytesWritten,
				TotalBytesRead:    totalRead.Load(),
				TotalWriteOps:     int64(fileCount),
				TotalReadOps:      totalOps.Load(),
				FileCount:         fileCount,
				FileSize:          fileSize,
				TempDir:           tmpDir,
			},
		}
		if ctx.Err() != nil && throttleCtx.Err() == context.Canceled {
			result.Err = ctx.Err()
		}

		handle.Send(result)
	}()

	return handle, nil
}

// emitPhaseEvents emits timestamped events at ramp phase boundaries.
func (s *Stress) emitPhaseEvents(ctx context.Context, globalStart time.Time, targetRate int64) {
	rampDownStart := s.Duration - s.RampDown

	if s.RampUp > 0 {
		s.emitEvent(trace.EventResourceRampUpStart,
			attribute.Int64(trace.AttrResourceTargetRate, targetRate),
			attribute.Int64(trace.AttrResourceRampUpMs, s.RampUp.Milliseconds()),
		)
		select {
		case <-time.After(s.RampUp):
			s.emitEvent(trace.EventResourceRampUpComplete)
		case <-ctx.Done():
			return
		}
	}

	s.emitEvent(trace.EventResourceSustainStart,
		attribute.Int64(trace.AttrResourceTargetRate, targetRate),
	)

	if s.RampDown > 0 {
		sustainDuration := rampDownStart - s.RampUp
		if sustainDuration > 0 {
			select {
			case <-time.After(sustainDuration):
			case <-ctx.Done():
				return
			}
		}

		s.emitEvent(trace.EventResourceRampDownStart,
			attribute.Int64(trace.AttrResourceRampDownMs, s.RampDown.Milliseconds()),
		)
		select {
		case <-time.After(s.RampDown):
			s.emitEvent(trace.EventResourceRampDownComplete)
		case <-ctx.Done():
			return
		}
	}
}

// writeRandomFile creates a file with n bytes of crypto/rand data.
func writeRandomFile(path string, n int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return err
	}
	_, err = f.Write(buf)
	return err
}

// readWorker reads files round-robin, acquiring tokens before each read.
func readWorker(ctx context.Context, bucket *tokenBucket, files []string, fileSize int, workerID int, totalRead, totalOps *atomic.Int64) {
	n := len(files)
	buf := make([]byte, fileSize)
	idx := workerID % n

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Acquire tokens for one file read.
		if !bucket.acquire(ctx, int64(fileSize)) {
			return // context cancelled while waiting
		}

		f, err := os.Open(files[idx])
		if err != nil {
			// File may have been cleaned up; exit gracefully.
			return
		}
		nRead, _ := f.Read(buf)
		f.Close()

		totalRead.Add(int64(nRead))
		totalOps.Add(1)

		idx = (idx + 1) % n
	}
}

// ── Token bucket ────────────────────────────────────────────────────

type tokenBucket struct {
	mu       sync.Mutex
	tokens   int64
	rate     int64 // bytes/sec (current, may be updated by ramp)
	capacity int64
	minCap   int64 // minimum capacity (must be >= max single acquire)
	lastTick time.Time
}

func newTokenBucket(rate int64, minCapacity int64) *tokenBucket {
	cap := bucketCapacity(rate, minCapacity)
	return &tokenBucket{
		tokens:   cap, // start full
		rate:     rate,
		capacity: cap,
		minCap:   minCapacity,
		lastTick: time.Now(),
	}
}

// bucketCapacity returns 2× the per-tick refill, but at least minCap so that
// a single acquire(fileSize) can always succeed.
func bucketCapacity(rate int64, minCap int64) int64 {
	perTick := rate * int64(RefillInterval) / int64(time.Second)
	if perTick < 1 {
		perTick = 1
	}
	cap := perTick * 2
	if cap < minCap {
		cap = minCap
	}
	return cap
}

// acquire blocks until n tokens are available or ctx is cancelled.
func (tb *tokenBucket) acquire(ctx context.Context, n int64) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		tb.mu.Lock()
		tb.refill()
		if tb.tokens >= n {
			tb.tokens -= n
			tb.mu.Unlock()
			return true
		}
		tb.mu.Unlock()

		// Wait a tick before retrying.
		t := time.NewTimer(RefillInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

// refill adds tokens based on elapsed time since last tick. Must be called with mu held.
func (tb *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastTick)
	if elapsed <= 0 {
		return
	}
	tb.lastTick = now

	added := tb.rate * int64(elapsed) / int64(time.Second)
	tb.tokens += added
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// setRate updates the refill rate and capacity (called by ramp controller).
func (tb *tokenBucket) setRate(newRate int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.rate = newRate
	tb.capacity = bucketCapacity(newRate, tb.minCap)
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// ── Ramp controller ─────────────────────────────────────────────────

// rampLoop adjusts the token bucket's refill rate over the ramp-up and
// ramp-down phases, sampling at RefillInterval.
func rampLoop(ctx context.Context, bucket *tokenBucket, targetRate int64, totalDuration, rampUp, rampDown time.Duration, globalStart time.Time) {
	if rampUp == 0 && rampDown == 0 {
		// No ramping needed — just wait for context to finish.
		<-ctx.Done()
		return
	}

	rampDownStart := totalDuration - rampDown
	ticker := time.NewTicker(RefillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		elapsed := time.Since(globalStart)
		rate := float64(targetRate)

		if rampUp > 0 && elapsed < rampUp {
			rate = float64(targetRate) * (float64(elapsed) / float64(rampUp))
		} else if rampDown > 0 && elapsed >= rampDownStart {
			progress := float64(elapsed-rampDownStart) / float64(rampDown)
			if progress > 1.0 {
				progress = 1.0
			}
			rate = float64(targetRate) * (1.0 - progress)
		}

		effectiveRate := int64(rate)
		if effectiveRate < 1 {
			effectiveRate = 1
		}
		bucket.setRate(effectiveRate)
	}
}

// Compile-time interface checks.
var (
	_ fault.Fault      = (*Stress)(nil)
	_ fault.EventAware = (*Stress)(nil)
)
