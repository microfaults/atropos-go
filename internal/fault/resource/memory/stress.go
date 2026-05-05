package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
)

// pageSize is the OS page size used for page-touching allocations.
const pageSize = 4096

// thrashStride is the byte stride for thrashing access. Set to pageSize+1
// so every access crosses a page boundary, maximising TLB misses.
const thrashStride = pageSize + 1

// Stress is a memory-pressure fault that consumes a configurable fraction
// of available memory using chunk-based allocation.
//
// Phase 1 (Allocate): gradually allocates byte-slice chunks until the
// target consumption is reached. Each chunk is page-touched to commit
// real RSS.
//
// Phase 2 (Hold + optional Thrash): holds allocations for the remaining
// duration. If Thrashing is enabled, worker goroutines continuously walk
// the allocated pages in a TLB-hostile stride pattern.
type Stress struct {
	Config
}

// Detail carries memory-specific diagnostics from a completed fault.
type Detail struct {
	AvailableMemory  uint64
	TargetBytes      uint64
	PeakAllocated    uint64
	ChunksAllocated  int
	ThrashingEnabled bool
}

// Validate checks that the memory stress config is valid.
func (s *Stress) Validate() error {
	return s.Config.Validate()
}

// Start begins memory stress in the background and returns immediately.
//
// The stress allocates memory in chunks, touching every page to force
// real RSS commitment. Load follows the ramp-up → steady → ramp-down
// timeline.
//
// Safety: memory limit is auto-detected from cgroup (Docker/k8s) or
// /proc/meminfo. Duration is enforced via context.WithTimeout.
func (s *Stress) Start(ctx context.Context) (*fault.Handle, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	available := AvailableMemory()
	currentUsage := CurrentUsage()
	chunkSize := s.EffectiveChunkSize()
	thrashWorkers := s.EffectiveThrashWorkers()

	// Targeting is INCREMENTAL: TargetLoad × available is the additional
	// pressure we add on top of baseline, not an absolute total to drive to.
	// See resource/config.go and ambiguities.md A15 for design notes.
	if available == 0 {
		return nil, fmt.Errorf("memory stress: AvailableMemory() returned 0; cannot determine capacity")
	}
	if currentUsage >= available {
		// Either detection sources are inconsistent (cgroup CurrentUsage
		// paired with /proc-fallback AvailableMemory under an unlimited
		// cgroup) or the system is genuinely at capacity. We cannot
		// allocate safely; refuse to start so the caller can log it.
		return nil, fmt.Errorf("memory stress: detection inconsistent or capacity exhausted (currentUsage=%d >= available=%d); refusing to start", currentUsage, available)
	}

	targetBytes := uint64(s.TargetLoad * float64(available))
	// OOM safety cap. Incremental targeting means we never reduce targetBytes
	// because of high baseline; we only cap to avoid pushing total past
	// capacity. After the currentUsage >= available guard above, the
	// subtraction here is always non-negative.
	if currentUsage+targetBytes > available {
		targetBytes = available - currentUsage
	}

	// How many chunks to reach target.
	numChunks := int(targetBytes) / chunkSize
	if numChunks < 1 && targetBytes > 0 {
		numChunks = 1
	}

	stressCtx, cancel := context.WithTimeout(ctx, s.Duration)
	handle := fault.NewHandle(cancel)

	go func() {
		defer cancel()

		start := time.Now()
		chunks := make([][]byte, 0, numChunks)
		var peakAllocated uint64

		// ── Phase 1: Allocate ──────────────────────────────────────
		// During ramp-up, spread allocations linearly over the ramp
		// period. Without ramp-up, allocate as fast as possible.
		allocInterval := time.Duration(0)
		if s.RampUp > 0 && numChunks > 1 {
			allocInterval = s.RampUp / time.Duration(numChunks)
		}

		for i := 0; i < numChunks; i++ {
			select {
			case <-stressCtx.Done():
				goto done
			default:
			}

			// If ramping, wait before each allocation.
			if allocInterval > 0 && i > 0 {
				t := time.NewTimer(allocInterval)
				select {
				case <-stressCtx.Done():
					t.Stop()
					goto done
				case <-t.C:
				}
			}

			// Allocate and page-touch the chunk.
			chunk := allocChunk(chunkSize)
			chunks = append(chunks, chunk)
			peakAllocated += uint64(chunkSize)
		}

		// ── Phase 2: Hold + optional Thrash ────────────────────────
		{
			var thrashWg sync.WaitGroup
			if s.Thrashing && len(chunks) > 0 {
				for w := 0; w < thrashWorkers; w++ {
					thrashWg.Add(1)
					go func(workerID int) {
						defer thrashWg.Done()
						thrashLoop(stressCtx, chunks, workerID)
					}(w)
				}
			}

			// Wait for ramp-down phase or context end.
			holdDuration := s.Duration - s.RampUp - s.RampDown
			if holdDuration > 0 {
				t := time.NewTimer(holdDuration)
				select {
				case <-stressCtx.Done():
					t.Stop()
				case <-t.C:
				}
			}

			// ── Ramp-down: release chunks linearly ─────────────────
			if s.RampDown > 0 && len(chunks) > 0 {
				releaseInterval := s.RampDown / time.Duration(len(chunks))
				for len(chunks) > 0 {
					select {
					case <-stressCtx.Done():
						goto cleanup
					default:
					}

					// Release the last chunk.
					chunks[len(chunks)-1] = nil
					chunks = chunks[:len(chunks)-1]

					if releaseInterval > 0 && len(chunks) > 0 {
						t := time.NewTimer(releaseInterval)
						select {
						case <-stressCtx.Done():
							t.Stop()
							goto cleanup
						case <-t.C:
						}
					}
				}
			}

		cleanup:
			// Signal thrash workers to stop (context is done or ramp
			// finished) and wait for them.
			cancel()
			thrashWg.Wait()
		}

	done:
		// Release all remaining chunks.
		for i := range chunks {
			chunks[i] = nil
		}
		chunks = nil

		elapsed := time.Since(start)

		result := fault.Result{
			ActualDuration: elapsed,
			Detail: Detail{
				AvailableMemory:  available,
				TargetBytes:      targetBytes,
				PeakAllocated:    peakAllocated,
				ChunksAllocated:  numChunks,
				ThrashingEnabled: s.Thrashing,
			},
		}
		if ctx.Err() != nil && stressCtx.Err() == context.Canceled {
			result.Err = ctx.Err()
		}

		handle.Send(result)
	}()

	return handle, nil
}

// allocChunk allocates a byte slice and touches every page to ensure
// the OS backs virtual pages with physical frames (real RSS growth).
func allocChunk(size int) []byte {
	buf := make([]byte, size)
	// Write one byte per page to force physical commitment.
	for off := 0; off < size; off += pageSize {
		buf[off] = 0xFF
	}
	return buf
}

// thrashLoop continuously walks allocated chunks with a stride that
// crosses page boundaries, maximising TLB misses and page-table walks.
// Each worker starts at a different offset to spread the pressure.
func thrashLoop(ctx context.Context, chunks [][]byte, workerID int) {
	n := len(chunks)
	if n == 0 {
		return
	}

	chunkIdx := workerID % n
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		chunk := chunks[chunkIdx]
		if chunk == nil {
			// Chunk was released during ramp-down.
			chunkIdx = (chunkIdx + 1) % n
			continue
		}

		size := len(chunk)
		// Stride across the chunk, doing read-modify-write to prevent
		// the compiler from optimising the access away.
		for off := 0; off < size; off += thrashStride {
			chunk[off] ^= 0xAA
		}

		chunkIdx = (chunkIdx + 1) % n
	}
}
