package atropos

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CachePushConfig configures a CachePushClient.
type CachePushConfig struct {
	BaseURL  string
	Service  string
	Instance string
	MaxBatch int           // default 100
	MaxWait  time.Duration // default 5s
	Client   *http.Client  // default http.DefaultClient
	Logger   *slog.Logger  // default slog.Default()

	// Fidelity, if set, receives per-(experiment_id, phase_id)
	// record_pushed/record_dropped/push_rejected_terminal counts (ATRO-7,
	// design doc Q6). It MUST be the same registry the CacheBox this
	// client pushes for uses -- the drain report snapshots ONE registry,
	// so a split leaves the report's push-side counts at zero.
	//
	// The construction order is circular for external callers (the
	// CacheBox needs this client's PushFunc; this client needs the
	// CacheBox's registry, whose internal type external modules cannot
	// name). Two ways out:
	//   - in-module: build a registry first and pass it to both this
	//     config and cachebox.Config.Fidelity;
	//   - external hosts: leave this nil, build the CacheBox with
	//     PushFunc(), then call BindFidelity(cb.Fidelity()) BEFORE any
	//     traffic flows.
	Fidelity *cachebox.FidelityRegistry
}

// CachePushStats reports push counters.
type CachePushStats struct {
	Pushed  int64
	Dropped int64
	// PushRejectedTerminal counts batches manteion rejected with
	// 409 phase_not_recording -- a terminal outcome (no retry), distinct
	// from a retried-then-exhausted transport/5xx failure.
	PushRejectedTerminal int64
	BatchesSent          int64
}

// CachePushClient batches cache entries and POSTs them to manteion's
// /api/v1/cache/ingest endpoint. It implements the cachebox.PushFunc
// signature so it can be wired into CacheBox.Config.Push.
//
// Batching strategy: flush when the batch reaches MaxBatch entries OR when
// MaxWait time elapses since the first entry was added, whichever comes
// first, OR when an entry's (ExperimentID, PhaseID) differs from the
// current batch's -- an envelope must never mix entries from two phases
// (wire spec §W2), so a phase transition forces an early flush.
//
// batch_seq is monotonic per (experiment, phase) pair (design doc Q2) and
// restarts whenever the tracked pair changes.
//
// Failure policy (design doc Q2): up to 3 attempts with exponential
// backoff (base 250ms + jitter) on transport errors and 5xx. A
// 409 phase_not_recording response stops retrying immediately
// (PushRejectedTerminal). Any other terminal outcome (retries exhausted,
// or a non-2xx/non-409 status) counts Dropped.
//
// Lifecycle: Stop() flushes the pending batch synchronously with a 5s
// timeout, then prevents further adds. Flush() does the same without
// stopping -- used at recording-phase end (ATRO-5) before a drain report.
type CachePushClient struct {
	baseURL  string
	service  string
	instance string
	client   *http.Client
	logger   *slog.Logger
	maxBatch int
	maxWait  time.Duration
	fidelity *cachebox.FidelityRegistry

	mu           sync.Mutex
	batch        []cachebox.WireEntry
	batchExpID   string
	batchPhaseID string
	batchSeq     int
	timer        *time.Timer
	stopped      bool

	// inflight tracks posts launched asynchronously by flushLocked so that
	// Flush/Stop can wait for them: a drain report built while a batch is
	// still mid-retry would under-count pushed entries and race manteion's
	// received count (INV-3's per-instance identity).
	inflight sync.WaitGroup

	pushed               atomic.Int64
	dropped              atomic.Int64
	pushRejectedTerminal atomic.Int64
	batchesSent          atomic.Int64
}

const (
	defaultMaxBatch = 100
	defaultMaxWait  = 5 * time.Second
	flushTimeout    = 5 * time.Second

	maxPushAttempts = 3
	pushBackoffBase = 250 * time.Millisecond
)

// NewCachePushClient constructs a CachePushClient from the given config.
// Unset fields get sensible defaults.
func NewCachePushClient(cfg CachePushConfig) *CachePushClient {
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = defaultMaxBatch
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = defaultMaxWait
	}
	if cfg.Client == nil {
		cfg.Client = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &CachePushClient{
		baseURL:  cfg.BaseURL,
		service:  cfg.Service,
		instance: cfg.Instance,
		client:   cfg.Client,
		logger:   cfg.Logger,
		maxBatch: cfg.MaxBatch,
		maxWait:  cfg.MaxWait,
		fidelity: cfg.Fidelity,
		batch:    make([]cachebox.WireEntry, 0, cfg.MaxBatch),
	}
}

// PushFunc returns a cachebox.PushFunc that feeds entries into this client.
func (c *CachePushClient) PushFunc() cachebox.PushFunc {
	return func(key string, entry *cachebox.Entry) {
		c.add(entry)
	}
}

// BindFidelity points this client's per-pair counters at reg -- pass
// CacheBox.Fidelity() so push-side and record-side counts land in the one
// registry the drain report snapshots. This is the external-host half of
// the circular construction described on CachePushConfig.Fidelity; it must
// be called before any traffic flows (counts recorded before the bind are
// lost to the report).
func (c *CachePushClient) BindFidelity(reg *cachebox.FidelityRegistry) {
	c.mu.Lock()
	c.fidelity = reg
	c.mu.Unlock()
}

// fid returns the current fidelity registry under the lock; post() snapshots
// it once so BindFidelity cannot race the counter calls.
func (c *CachePushClient) fid() *cachebox.FidelityRegistry {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fidelity
}

// Stats returns a snapshot of push counters.
func (c *CachePushClient) Stats() CachePushStats {
	return CachePushStats{
		Pushed:               c.pushed.Load(),
		Dropped:              c.dropped.Load(),
		PushRejectedTerminal: c.pushRejectedTerminal.Load(),
		BatchesSent:          c.batchesSent.Load(),
	}
}

func (c *CachePushClient) add(entry *cachebox.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}

	// A phase transition means we flush whatever's pending under the OLD
	// tag before starting a new batch under the new one -- an envelope
	// must never mix entries from two phases (wire spec §W2).
	if len(c.batch) > 0 && (entry.ExperimentID != c.batchExpID || entry.PhaseID != c.batchPhaseID) {
		c.flushLocked()
	}
	if len(c.batch) == 0 && (entry.ExperimentID != c.batchExpID || entry.PhaseID != c.batchPhaseID) {
		c.batchSeq = 0 // new pair -- batch_seq restarts (design doc Q2)
	}
	c.batchExpID = entry.ExperimentID
	c.batchPhaseID = entry.PhaseID

	c.batch = append(c.batch, cachebox.EntryToWire(entry))

	if len(c.batch) >= c.maxBatch {
		c.flushLocked()
		return
	}

	// Start timer on first entry in batch.
	if c.timer == nil {
		c.timer = time.AfterFunc(c.maxWait, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if !c.stopped && len(c.batch) > 0 {
				c.flushLocked()
			}
		})
	}
}

// flushLocked must be called with c.mu held. It takes ownership of the
// current batch slice and spawns a goroutine for the POST.
func (c *CachePushClient) flushLocked() {
	if len(c.batch) == 0 {
		return
	}
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	entries := c.batch
	expID, phaseID := c.batchExpID, c.batchPhaseID
	c.batchSeq++
	seq := c.batchSeq
	c.batch = make([]cachebox.WireEntry, 0, c.maxBatch)
	c.inflight.Add(1)
	go func() {
		defer c.inflight.Done()
		c.post(entries, expID, phaseID, seq)
	}()
}

// Flush immediately sends whatever is currently batched, without waiting
// for MaxBatch or MaxWait, and blocks until that push attempt AND every
// previously-launched asynchronous batch post (including retries) has
// completed. Unlike Stop, it does not prevent further adds. Used at
// recording-phase end (ATRO-5, design doc Q2) so a drain report can be
// built only after every entry handed to this client has been attempted
// -- a report that ignored in-flight batches would under-count pushed.
func (c *CachePushClient) Flush() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return // Stop already flushed and waited
	}
	var entries []cachebox.WireEntry
	var expID, phaseID string
	var seq int
	if len(c.batch) > 0 {
		if c.timer != nil {
			c.timer.Stop()
			c.timer = nil
		}
		entries = c.batch
		expID, phaseID = c.batchExpID, c.batchPhaseID
		c.batchSeq++
		seq = c.batchSeq
		c.batch = make([]cachebox.WireEntry, 0, c.maxBatch)
	}
	c.mu.Unlock()

	if len(entries) > 0 {
		c.post(entries, expID, phaseID, seq) // synchronous -- caller waits
	}
	c.inflight.Wait()
}

// ingestEnvelope is the POST body for /api/v1/cache/ingest (wire spec §W2).
// BatchSeq is 1-based and monotonic per (experiment_id, phase_id, instance)
// so manteion can dedupe retried batches.
type ingestEnvelope struct {
	Service      string               `json:"service"`
	Instance     string               `json:"instance"`
	PhaseID      string               `json:"phase_id"`
	ExperimentID string               `json:"experiment_id,omitempty"`
	BatchSeq     int                  `json:"batch_seq,omitempty"`
	Entries      []cachebox.WireEntry `json:"entries"`
}

// post sends the batch to manteion, retrying transport errors and 5xx up
// to maxPushAttempts times with exponential backoff + jitter. A
// 409 phase_not_recording response is terminal: it stops retrying and
// counts PushRejectedTerminal (in addition to Dropped, since the entries
// are lost either way). It runs WITHOUT the lock held (I/O-bound) --
// called via `go` from flushLocked (fire-and-forget) or directly from
// Flush/Stop (synchronous, caller waits).
func (c *CachePushClient) post(entries []cachebox.WireEntry, experimentID, phaseID string, batchSeq int) {
	pair := cachebox.PhasePair{ExperimentID: experimentID, PhaseID: phaseID}
	fidelity := c.fid() // snapshot once: BindFidelity may not race the unlocked reads below
	body, err := json.Marshal(ingestEnvelope{
		Service:      c.service,
		Instance:     c.instance,
		PhaseID:      phaseID,
		ExperimentID: experimentID,
		BatchSeq:     batchSeq,
		Entries:      entries,
	})
	if err != nil {
		c.logger.Warn("cache push marshal error", "error", err)
		c.dropped.Add(int64(len(entries)))
		fidelity.RecordDropped(pair, int64(len(entries)))
		return
	}

	for attempt := 1; attempt <= maxPushAttempts; attempt++ {
		status, doErr := c.attempt(body)

		if doErr == nil && status == http.StatusConflict {
			c.logger.Warn("cache push rejected: phase not recording",
				"batch_seq", batchSeq, "experiment_id", experimentID, "phase_id", phaseID)
			c.pushRejectedTerminal.Add(1)
			c.dropped.Add(int64(len(entries)))
			fidelity.RecordPushRejectedTerminal(pair, int64(len(entries)))
			fidelity.RecordDropped(pair, int64(len(entries)))
			return
		}

		if doErr == nil && status >= 200 && status < 300 {
			c.pushed.Add(int64(len(entries)))
			c.batchesSent.Add(1)
			fidelity.RecordPushed(pair, int64(len(entries)))
			return
		}

		retryable := doErr != nil || status >= 500
		if doErr != nil {
			c.logger.Warn("cache push failed", "error", doErr, "attempt", attempt)
		} else {
			c.logger.Warn("cache push rejected", "status", status, "attempt", attempt)
		}
		if !retryable || attempt == maxPushAttempts {
			break
		}
		time.Sleep(pushBackoff(attempt))
	}

	c.dropped.Add(int64(len(entries)))
	fidelity.RecordDropped(pair, int64(len(entries)))
}

// attempt makes one POST attempt and returns (status, error). status is
// only meaningful when error is nil.
func (c *CachePushClient) attempt(body []byte) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/cache/ingest", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

// pushBackoff returns the exponential backoff (base pushBackoffBase) plus
// full jitter for the given 1-based attempt number.
func pushBackoff(attempt int) time.Duration {
	backoff := pushBackoffBase * time.Duration(uint64(1)<<uint(attempt-1))
	return backoff + rand.N(backoff)
}

// Stop flushes the pending batch synchronously and prevents further adds.
// The post method has its own 5s context timeout.
func (c *CachePushClient) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	entries := c.batch
	expID, phaseID, seq := c.batchExpID, c.batchPhaseID, c.batchSeq+1
	c.batch = nil
	c.mu.Unlock()

	if len(entries) > 0 {
		c.post(entries, expID, phaseID, seq) // synchronous — blocks until POST completes or times out
	}
	c.inflight.Wait()
}

// SendDrainReport settles the full record pipeline for (experimentID,
// phaseID) and POSTs a W3 drain report, retrying up to maxPushAttempts
// times on transport errors and 5xx. Settling order matters: first the
// recorder's queue (so every accepted record reaches this client), then
// this client's pending batch and every in-flight post (so pushed/dropped
// are final). Only then is the pair's fidelity snapshot taken -- the
// report's counts are per-(experiment, phase), NOT process-lifetime; a
// second recording phase in the same process must not inherit the first
// phase's totals, and manteion's drain gate compares them against its
// per-pair received count (INV-3). Called when a rule-version change
// removes the recording context for this pair (ATRO-5(c)) -- detecting
// that transition is the caller's job (see CacheDrainTracker).
func (c *CachePushClient) SendDrainReport(experimentID, phaseID string, cb *CacheBox) DrainReportResponse {
	cb.FlushRecording()
	c.Flush()

	pair := cachebox.PhasePair{ExperimentID: experimentID, PhaseID: phaseID}
	fc := cb.Fidelity().Snapshot(pair)
	report := DrainReport{
		ExperimentID:           experimentID,
		PhaseID:                phaseID,
		Service:                c.service,
		InstanceID:             c.instance,
		EntriesRecorded:        fc.RecordEnqueued,
		EntriesPushed:          fc.RecordPushed,
		EntriesDropped:         fc.RecordDropped,
		BatchesSent:            c.batchesSent.Load(),
		LastBatchSeq:           int64(c.currentBatchSeq()),
		KeyCollisionsDivergent: fc.KeyCollisionsDivergent,
		KeyCollisionsIdentical: fc.KeyCollisionsIdentical,
	}
	body, err := json.Marshal(report)
	if err != nil {
		c.logger.Warn("drain report marshal error", "error", err)
		return DrainReportResponse{Accepted: false}
	}

	for attempt := 1; attempt <= maxPushAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			c.baseURL+"/api/v1/sdk/cachebox/drain", bytes.NewReader(body))
		if err != nil {
			cancel()
			c.logger.Warn("drain report request build error", "error", err)
			return DrainReportResponse{Accepted: false}
		}
		req.Header.Set("Content-Type", "application/json")

		resp, doErr := c.client.Do(req)
		cancel()
		if doErr != nil {
			c.logger.Warn("drain report post failed", "error", doErr, "attempt", attempt)
			if attempt < maxPushAttempts {
				time.Sleep(pushBackoff(attempt))
			}
			continue
		}

		var decoded DrainReportResponse
		_ = json.NewDecoder(resp.Body).Decode(&decoded)
		resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return decoded
		}
		c.logger.Warn("drain report rejected", "status", resp.StatusCode, "attempt", attempt)
		if resp.StatusCode < 500 {
			break // non-5xx rejection -- not retryable
		}
		if attempt < maxPushAttempts {
			time.Sleep(pushBackoff(attempt))
		}
	}
	return DrainReportResponse{Accepted: false}
}

// currentBatchSeq returns the last batch_seq assigned, for the drain
// report's LastBatchSeq field.
func (c *CachePushClient) currentBatchSeq() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.batchSeq
}
