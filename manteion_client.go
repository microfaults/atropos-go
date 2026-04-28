package atropos

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ManteionStatus represents the SDK's connectivity state with manteion.
type ManteionStatus int32

const (
	ManteionDisconnected ManteionStatus = iota // never connected (or offline)
	ManteionConnected                          // poll succeeding normally
	ManteionDegraded                           // recent poll failures; using stale rules
)

// ManteionClient manages the lifecycle of the SDK's connection to manteion:
// startup polling, registration, rule sync, and graceful shutdown.
type ManteionClient struct {
	cfg        manteionConfig
	httpClient *http.Client // used for all short-lived calls (poll, register, init)
	sseClient  *http.Client // no Timeout — SSE connections are indefinitely long-lived
	targets    ApplyTargets
	logger     *slog.Logger

	ruleVersion atomic.Uint64
	lastPollAt  atomic.Int64 // unix nanos; 0 = never polled
	status      atomic.Int32

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Status returns the current connectivity status.
func (c *ManteionClient) Status() ManteionStatus {
	if c == nil {
		return ManteionDisconnected
	}
	return ManteionStatus(c.status.Load())
}

// Close cancels the poll loop, waits for it to exit, and sends a best-effort
// deregister to manteion. Safe to call on a nil receiver (offline mode).
func (c *ManteionClient) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	// Best-effort deregister — don't fail shutdown for this.
	dctx, dcancel := context.WithTimeout(ctx, 2*time.Second)
	defer dcancel()
	deregisterURL := c.cfg.url + "/api/v1/sdk/register/" + url.PathEscape(c.cfg.instanceID)
	req, err := http.NewRequestWithContext(dctx, http.MethodDelete, deregisterURL, nil)
	if err != nil {
		c.logger.Warn("build deregister request failed", "error", err)
		return nil
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Warn("deregister failed", "error", err)
		return nil
	}
	resp.Body.Close()
	return nil
}

// waitForReady polls GET /api/v1/sdk/init with exponential backoff until it
// returns 200 or the context deadline is reached.
func (c *ManteionClient) waitForReady(ctx context.Context) error {
	deadline := time.Now().Add(c.cfg.initTimeout)
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.url+"/api/v1/sdk/init", nil)
		if err != nil {
			return fmt.Errorf("build init request: %w", err)
		}
		resp, err := c.httpClient.Do(req)
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				c.logger.Info("manteion ready", "attempts", attempt)
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("manteion not ready after %d attempts (%v): %w",
				attempt, c.cfg.initTimeout, ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// register calls RegisterWithClient then Apply to configure the evaluator.
func (c *ManteionClient) register(ctx context.Context) error {
	resp, err := RegisterWithClient(ctx, c.httpClient, c.cfg.url, RegisterRequest{
		ID:      c.cfg.instanceID,
		Service: c.cfg.serviceName,
		Version: c.cfg.serviceVersion,
		Address: os.Getenv("POD_IP"),
	})
	if err != nil {
		return err
	}
	return Apply(resp, c.targets)
}

// pollLoopWithTrigger runs until ctx is cancelled. It fetches rules every
// pollInterval or immediately when the SSE listener sends a trigger signal.
// Applies exponential backoff on failures and re-registers on recovery.
func (c *ManteionClient) pollLoopWithTrigger(ctx context.Context, trigger <-chan struct{}) {
	ticker := time.NewTicker(c.cfg.pollInterval)
	defer ticker.Stop()

	consecutiveFailures := 0

	doPoll := func() {
		err := c.fetchRules(ctx)
		if err != nil {
			consecutiveFailures++
			c.status.Store(int32(ManteionDegraded))

			backoff := c.cfg.pollInterval * time.Duration(1<<min(consecutiveFailures, 6))
			backoff = min(backoff, 60*time.Second)
			ticker.Reset(backoff)

			c.logger.Warn("manteion poll failed, continuing with stale rules",
				"failures", consecutiveFailures,
				"next_retry", backoff,
				"error", err,
			)

			if consecutiveFailures == 6 {
				go c.register(ctx)
			}
			return
		}

		if consecutiveFailures > 0 {
			c.logger.Info("manteion connection restored", "missed_polls", consecutiveFailures)
			// Force a full rule pull: manteion may have restarted or accumulated
			// changes we missed. Re-register with same instanceID (idempotent upsert).
			c.ruleVersion.Store(0)
			go c.register(ctx)
		}
		consecutiveFailures = 0
		c.lastPollAt.Store(time.Now().UnixNano())
		c.status.Store(int32(ManteionConnected))
		ticker.Reset(c.cfg.pollInterval)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			doPoll()
		case <-trigger:
			doPoll()
		}
	}
}

// fetchRules calls GET /api/v1/sdk/rules and applies any updated rules.
func (c *ManteionClient) fetchRules(ctx context.Context) error {
	q := url.Values{
		"service":     {c.cfg.serviceName},
		"version":     {strconv.FormatUint(c.ruleVersion.Load(), 10)},
		"instance_id": {c.cfg.instanceID},
	}
	fullURL := c.cfg.url + "/api/v1/sdk/rules?" + q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return fmt.Errorf("build rules request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		return nil

	case http.StatusOK:
		var payload struct {
			Version uint64         `json:"version"`
			Rules   []CompiledRule `json:"rules"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return fmt.Errorf("decode rules: %w", err)
		}

		staticRules, err := DecodeCompiledRules(payload.Rules)
		if err != nil {
			c.logger.Error("rule conversion failed, keeping stale rules", "error", err)
			return nil // manteion is alive; this is a data issue
		}

		c.targets.Evaluator.SetRules(staticRules)
		c.ruleVersion.Store(payload.Version)
		c.logger.Info("rules updated", "version", payload.Version, "count", len(staticRules))
		return nil

	case http.StatusServiceUnavailable:
		return fmt.Errorf("manteion not ready (503)")

	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
}

// listenSSE connects to GET /api/v1/sdk/events and triggers an immediate poll
// whenever a "rules_changed" event arrives. Reconnects silently on disconnect —
// the poll loop already handles degraded state logging, so SSE errors are only
// logged on the first failure per connection to avoid noise during outages.
func (c *ManteionClient) listenSSE(ctx context.Context, triggerPoll func()) {
	sseURL := c.cfg.url + "/api/v1/sdk/events?service=" + url.QueryEscape(c.cfg.serviceName)
	firstFailure := true
	for {
		if ctx.Err() != nil {
			return
		}
		err := c.sseStream(ctx, sseURL, triggerPoll)
		if ctx.Err() != nil {
			return
		}
		if err != nil && firstFailure {
			c.logger.Debug("SSE stream ended, falling back to poll-only", "error", err)
			firstFailure = false
		} else if err == nil {
			// clean disconnect (server restarted etc.) — reset so next failure logs
			firstFailure = true
		}
		// Brief pause before reconnect to avoid a tight reconnect loop.
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *ManteionClient) sseStream(ctx context.Context, sseURL string, triggerPoll func()) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return err
	}
	// sseClient has no Timeout so the connection can stream indefinitely.
	// The request context (cancelled by Close) bounds the lifetime.
	resp, err := c.sseClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SSE: unexpected status %d", resp.StatusCode)
	}

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "event: rules_changed") {
			triggerPoll()
		}
	}
	return sc.Err()
}

// generateInstanceID returns "${hostname}-${random8hex}".
func generateInstanceID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	b := make([]byte, 4)
	rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
