package atropos

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
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
	RunID    string        // experiment run this push belongs to
	MaxBatch int           // default 100
	MaxWait  time.Duration // default 5s
	Client   *http.Client  // default http.DefaultClient
	Logger   *slog.Logger  // default slog.Default()
}

// CachePushStats reports push counters.
type CachePushStats struct {
	Pushed  int64
	Dropped int64
}

// CachePushClient batches cache entries and POSTs them to manteion's
// /api/v1/cache/ingest endpoint. It implements the cachebox.PushFunc
// signature so it can be wired into CacheBox.Config.Push.
//
// Batching strategy: flush when the batch reaches MaxBatch entries OR when
// MaxWait time elapses since the first entry was added, whichever comes first.
//
// Failure policy: POST failure -> log + increment dropped counter -> drop
// batch. No retry.
//
// Lifecycle: Stop() flushes the pending batch synchronously with a 5s
// timeout, then prevents further adds.
type CachePushClient struct {
	baseURL  string
	service  string
	instance string
	runID    string
	client   *http.Client
	logger   *slog.Logger
	maxBatch int
	maxWait  time.Duration

	mu      sync.Mutex
	batch   []cachebox.WireEntry
	timer   *time.Timer
	stopped bool

	pushed  atomic.Int64
	dropped atomic.Int64
}

const (
	defaultMaxBatch = 100
	defaultMaxWait  = 5 * time.Second
	flushTimeout    = 5 * time.Second
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
		runID:    cfg.RunID,
		client:   cfg.Client,
		logger:   cfg.Logger,
		maxBatch: cfg.MaxBatch,
		maxWait:  cfg.MaxWait,
		batch:    make([]cachebox.WireEntry, 0, cfg.MaxBatch),
	}
}

// PushFunc returns a cachebox.PushFunc that feeds entries into this client.
func (c *CachePushClient) PushFunc() cachebox.PushFunc {
	return func(key string, entry *cachebox.Entry) {
		c.add(entry)
	}
}

// Stats returns a snapshot of push counters.
func (c *CachePushClient) Stats() CachePushStats {
	return CachePushStats{
		Pushed:  c.pushed.Load(),
		Dropped: c.dropped.Load(),
	}
}

func (c *CachePushClient) add(entry *cachebox.Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return
	}

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
	c.batch = make([]cachebox.WireEntry, 0, c.maxBatch)
	go c.post(entries)
}

type ingestEnvelope struct {
	Service  string               `json:"service"`
	Instance string               `json:"instance"`
	RunID    string               `json:"run_id"`
	Entries  []cachebox.WireEntry `json:"entries"`
}

// post sends the batch to manteion. It runs WITHOUT the lock held (it is
// I/O-bound and must not block adds).
func (c *CachePushClient) post(entries []cachebox.WireEntry) {
	body, err := json.Marshal(ingestEnvelope{
		Service:  c.service,
		Instance: c.instance,
		RunID:    c.runID,
		Entries:  entries,
	})
	if err != nil {
		c.logger.Warn("cache push marshal error", "error", err)
		c.dropped.Add(int64(len(entries)))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/api/v1/cache/ingest", bytes.NewReader(body))
	if err != nil {
		c.dropped.Add(int64(len(entries)))
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		c.logger.Warn("cache push failed", "error", err)
		c.dropped.Add(int64(len(entries)))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		c.logger.Warn("cache push rejected", "status", resp.StatusCode)
		c.dropped.Add(int64(len(entries)))
		return
	}

	c.pushed.Add(int64(len(entries)))
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
	c.batch = nil
	c.mu.Unlock()

	if len(entries) > 0 {
		c.post(entries) // synchronous — blocks until POST completes or times out
	}
}
