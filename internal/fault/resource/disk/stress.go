package disk

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"

	"go.opentelemetry.io/otel/attribute"
)

// Stress is a disk I/O fault that writes a single large temp file to
// pressure storage bandwidth.
//
// Phase 1 (grow):   appends chunks until the file reaches MaxDiskUsage,
//
//	occupying real disk space.
//
// Phase 2 (sustain): seeks back to 0 and overwrites in a loop for the
//
//	remainder of the duration, sustaining bandwidth pressure
//	without growing further.
//
// The write rate is throttled by a token bucket and follows the
// ramp-up → steady → ramp-down timeline from FaultConfig.
//
// Implements fault.Fault and fault.EventAware.
type Stress struct {
	Config
	emit fault.EventEmitter
}

// Detail carries disk-specific diagnostics from a completed fault.
type Detail struct {
	TotalBytesWritten int64
	TotalWriteOps     int64
	MaxDiskUsage      int64
	TempFile          string
}

// SetEventEmitter implements fault.EventAware.
func (s *Stress) SetEventEmitter(fn fault.EventEmitter) {
	s.emit = fn
}

func (s *Stress) emitEvent(name string, attrs ...attribute.KeyValue) {
	if s.emit != nil {
		s.emit(name, attrs...)
	}
}

func (s *Stress) Validate() error {
	return s.Config.Validate()
}

// Start creates a single temp file and begins writing to it in the background.
// Returns immediately with a Handle; the caller waits on handle.Done().
func (s *Stress) Start(ctx context.Context) (*fault.Handle, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	writeRate := s.EffectiveWriteRate()
	maxDisk := s.EffectiveMaxDiskUsage()
	chunkSize := s.EffectiveChunkSize()
	basePath := s.EffectivePath()

	// Create the single temp file synchronously so callers get an error fast.
	f, err := os.CreateTemp(basePath, "atropos-disk-*.bin")
	if err != nil {
		return nil, fmt.Errorf("disk: failed to create temp file: %w", err)
	}
	tmpPath := f.Name()

	throttleCtx, cancel := context.WithTimeout(ctx, s.Duration)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()
		defer os.Remove(tmpPath)
		defer f.Close()

		var (
			totalWritten atomic.Int64
			totalOps     atomic.Int64
			wg           sync.WaitGroup
		)

		bucket := newTokenBucket(writeRate, chunkSize)
		start := time.Now()

		// Ramp controller — adjusts token bucket rate over the fault timeline.
		wg.Add(1)
		go func() {
			defer wg.Done()
			rampLoop(throttleCtx, bucket, writeRate, s.Duration, s.RampUp, s.RampDown, start)
		}()

		// Phase event emitter — mirrors cpu.Stress.emitPhaseEvents pattern.
		if s.emit != nil && (s.RampUp > 0 || s.RampDown > 0) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.emitPhaseEvents(throttleCtx, start, writeRate)
			}()
		}

		// Single write worker.
		wg.Add(1)
		go func() {
			defer wg.Done()
			writeWorker(throttleCtx, bucket, f, maxDisk, chunkSize, &totalWritten, &totalOps)
		}()

		wg.Wait()
		elapsed := time.Since(start)

		result := fault.Result{
			ActualDuration: elapsed,
			Detail: Detail{
				TotalBytesWritten: totalWritten.Load(),
				TotalWriteOps:     totalOps.Load(),
				MaxDiskUsage:      maxDisk,
				TempFile:          tmpPath,
			},
		}
		if ctx.Err() != nil && throttleCtx.Err() == context.Canceled {
			result.Err = ctx.Err()
		}

		handle.Send(result)
	}()

	return handle, nil
}

// emitPhaseEvents mirrors the cpu.Stress pattern exactly.
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

// writeWorker writes a single file in two phases:
//  1. Grow: append chunks until the file reaches maxDisk bytes.
//  2. Sustain: seek to 0 and overwrite in a loop to keep pressuring bandwidth.
func writeWorker(ctx context.Context, bucket *tokenBucket, f *os.File, maxDisk, chunkSize int64, totalWritten, totalOps *atomic.Int64) {
	chunk := make([]byte, chunkSize)
	var fileSize int64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !bucket.acquire(ctx, chunkSize) {
			return
		}

		if fileSize+chunkSize > maxDisk {
			if _, err := f.Seek(0, 0); err != nil {
				return
			}
			fileSize = 0
		}

		n, err := f.Write(chunk)
		if err != nil {
			return
		}

		fileSize += int64(n)
		totalWritten.Add(int64(n))
		totalOps.Add(1)
	}
}

// ── Token bucket ─────────────────────────────────────────────────────────────

type tokenBucket struct {
	mu       sync.Mutex
	tokens   int64
	rate     int64
	capacity int64
	minCap   int64
	lastTick time.Time
}

func newTokenBucket(rate, minCapacity int64) *tokenBucket {
	cap := bucketCapacity(rate, minCapacity)
	return &tokenBucket{
		tokens:   cap,
		rate:     rate,
		capacity: cap,
		minCap:   minCapacity,
		lastTick: time.Now(),
	}
}

func bucketCapacity(rate, minCap int64) int64 {
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

		t := time.NewTimer(RefillInterval)
		select {
		case <-ctx.Done():
			t.Stop()
			return false
		case <-t.C:
		}
	}
}

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

func (tb *tokenBucket) setRate(newRate int64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.rate = newRate
	tb.capacity = bucketCapacity(newRate, tb.minCap)
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
}

// ── Ramp controller ──────────────────────────────────────────────────────────

func rampLoop(ctx context.Context, bucket *tokenBucket, targetRate int64, totalDuration, rampUp, rampDown time.Duration, globalStart time.Time) {
	if rampUp == 0 && rampDown == 0 {
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
