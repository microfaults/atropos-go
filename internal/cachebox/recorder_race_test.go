package cachebox

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
)

// TestRecorder_RecordFlushRaceWithStop is a regression test for a
// "send on closed channel" panic: both Record and Flush check r.stopped and
// then send on r.ch as two separate steps, so a concurrent Stop that closes
// r.ch in between the check and the send makes the send panic.
//
// It launches many concurrent Record and Flush callers, releases them all at
// once, and closes the recorder (Stop) while they are mid-flight, repeating
// over many iterations to make the interleaving likely. Any panic in a caller
// is captured and fails the test. Run under the race detector to also surface
// the underlying synchronization bug:
//
//	go test -race -run TestRecorder_RecordFlushRaceWithStop ./internal/cachebox/
func TestRecorder_RecordFlushRaceWithStop(t *testing.T) {
	const (
		iterations = 200
		recorders  = 8
		flushers   = 4
		callsEach  = 200
	)

	for it := 0; it < iterations; it++ {
		r := NewRecorder(RecorderConfig{
			Store:   NewMemStore(MemStoreConfig{}),
			KeyFunc: KeyFuncFor(KeyStrategyExact),
			BufSize: 8,
		})

		var (
			wg        sync.WaitGroup
			panicked  atomic.Bool
			panicText atomic.Value // string
			start     = make(chan struct{})
		)

		// capture runs fn after the start barrier, recovering (and recording)
		// any panic so a "send on closed channel" surfaces as a test failure
		// rather than crashing the whole test binary.
		capture := func(fn func()) {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					panicked.Store(true)
					panicText.Store(fmt.Sprint(p))
				}
			}()
			<-start
			fn()
		}

		req := mustRequest(t, "GET", "http://svc/x")

		wg.Add(recorders + flushers)
		for i := 0; i < recorders; i++ {
			go capture(func() {
				for j := 0; j < callsEach; j++ {
					r.Record(CacheRecord{Request: req, ResponseHeader: http.Header{}})
				}
			})
		}
		for i := 0; i < flushers; i++ {
			go capture(func() {
				for j := 0; j < callsEach; j++ {
					r.Flush()
				}
			})
		}

		close(start) // release all callers simultaneously
		r.Stop()     // close r.ch while callers are mid-flight
		wg.Wait()

		if panicked.Load() {
			t.Fatalf("iteration %d: Record/Flush panicked racing Stop: %v",
				it, panicText.Load())
		}
	}
}
