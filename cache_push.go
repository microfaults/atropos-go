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
	// PhaseID is a legacy fallback only. Since ATRO-5, each pushed entry
	// carries its own ExperimentID/PhaseID (stamped from the matched rule's
	// CacheBoxContext at record time -- see cacheBoxPassthrough); the
	// envelope is built from the entries actually in the batch, not this
	// field. Left in place only for source compatibility with existing
	// SetPhaseID/SetRunID callers.
	PhaseID  string
	MaxBatch int           // default 100
	MaxWait  time.Duration // default 5s
	Client   *http.Client  // default http.DefaultClient
	Logger   *slog.Logger  // default slog.Default()

	// Fidelity, if set, receives per-(experiment_id, phase_id)
	// record_pushed/record_dropped/push_rejected_terminal counts (ATRO-7,
	// design doc Q6). Pass the same registry the CacheBox this client
	// pushes for was built with (CacheBox.Fidelity()) so both sides of the
	// record/replay/push pipeline land in one shared per-pair view.
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
	phaseID  string // legacy fallback; see CachePushConfig.PhaseID
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
		phaseID:  cfg.PhaseID,
		client:   cfg.Client,
		logger:   cfg.Logger,
		maxBatch: cfg.MaxBatch,
		maxWait:  cfg.MaxWait,
		fidelity: cfg.Fidelity,
		batch:    make([]cachebox.WireEntry, 0, cfg.MaxBatch),
	}
}

// SetPhaseID updates the legacy fallback phase ID.
//
// Deprecated: since ATRO-5, each entry carries its own (experiment_id,
// phase_id) stamped from the matched rule's CacheBoxContext at record
// time; this ambient value is no longer consulted by add/post. Kept only
// for source compatibility with existing callers.
func (c *CachePushClient) SetPhaseID(phaseID string) {
	c.mu.Lock()
	c.phaseID = phaseID
	c.mu.Unlock()
}

// SetRunID updates the phase ID after construction.
//
// Deprecated: runs were renamed to phases when manteion moved to the
// phase-first experiment model; use SetPhaseID (itself deprecated -- see
// its doc).
func (c *CachePushClient) SetRunID(id string) { c.SetPhaseID(id) }

// PushFunc returns a cachebox.PushFunc that feeds entries into this client.
func (c *CachePushClient) PushFunc() cachebox.PushFunc {
	return func(key string, entry *cachebox.Entry) {
		c.add(entry)
	}
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
	go c.post(entries, expID, phaseID, seq)
}

// Flush immediately sends whatever is currently batched, without waiting
// for MaxBatch or MaxWait, and blocks until that push attempt (including
// retries) completes. Unlike Stop, it does not prevent further adds. Used
// at recording-phase end (ATRO-5, design doc Q2) so a drain report can be
// built only after every batched entry has been attempted.
func (c *CachePushClient) Flush() {
	c.mu.Lock()
	if c.stopped || len(c.batch) == 0 {
		c.mu.Unlock()
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
	c.mu.Unlock()

	c.post(entries, expID, phaseID, seq) // synchronous, not `go` -- caller waits
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
		c.fidelity.RecordDropped(pair, int64(len(entries)))
		return
	}

	for attempt := 1; attempt <= maxPushAttempts; attempt++ {
		status, doErr := c.attempt(body)

		if doErr == nil && status == http.StatusConflict {
			c.logger.Warn("cache push rejected: phase not recording",
				"batch_seq", batchSeq, "experiment_id", experimentID, "phase_id", phaseID)
			c.pushRejectedTerminal.Add(1)
			c.dropped.Add(int64(len(entries)))
			c.fidelity.RecordPushRejectedTerminal(pair, int64(len(entries)))
			c.fidelity.RecordDropped(pair, int64(len(entries)))
			return
		}

		if doErr == nil && status >= 200 && status < 300 {
			c.pushed.Add(int64(len(entries)))
			c.batchesSent.Add(1)
			c.fidelity.RecordPushed(pair, int64(len(entries)))
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
	c.fidelity.RecordDropped(pair, int64(len(entries)))
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
}

// SendDrainReport flushes any pending batch, then builds and POSTs a W3
// drain report for (experimentID, phaseID), retrying up to
// maxPushAttempts times on transport errors and 5xx. cb supplies
// entries-recorded and this pair's collision counts (via its fidelity
// registry, design doc Q2/Q6); this client supplies the push-side counts.
// Called when a rule-version change removes the recording context for
// this pair (ATRO-5(c)) -- detecting that transition is the caller's job
// (see CacheDrainTracker).
func (c *CachePushClient) SendDrainReport(experimentID, phaseID string, cb *CacheBox) DrainReportResponse {
	c.Flush()

	pair := cachebox.PhasePair{ExperimentID: experimentID, PhaseID: phaseID}
	fc := cb.Fidelity().Snapshot(pair)
	report := DrainReport{
		ExperimentID:           experimentID,
		PhaseID:                phaseID,
		Service:                c.service,
		InstanceID:             c.instance,
		EntriesRecorded:        cb.RecorderStats().Recorded,
		EntriesPushed:          c.pushed.Load(),
		EntriesDropped:         c.dropped.Load(),
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
