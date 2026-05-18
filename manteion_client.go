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

	pollCtx context.Context // cancelled by Close; passed to register goroutines
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// newReq builds a manteion-bound request with auth applied.
// Single chokepoint for all outgoing manteion requests in this client.
func (c *ManteionClient) newReq(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if c.cfg.authFn != nil {
		if err := c.cfg.authFn(req); err != nil {
			return nil, fmt.Errorf("auth: %w", err)
		}
	}
	return req, nil
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
// Always returns nil today (best-effort); reserved for future fatal-error reporting.
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
	req, err := c.newReq(dctx, http.MethodDelete, deregisterURL, nil)
	if err != nil {
		c.logger.Warn("build deregister request failed", "error", err)
		return nil
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.logger.Warn("deregister failed", "error", err)
		return nil
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		c.logger.Warn("deregister returned non-2xx", "status", resp.StatusCode)
	}
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
		req, err := c.newReq(ctx, http.MethodGet, c.cfg.url+"/api/v1/sdk/init", nil)
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
			if resp.StatusCode != http.StatusServiceUnavailable {
				c.logger.Warn("manteion init returned unexpected status",
					"status", resp.StatusCode, "attempt", attempt)
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
		ID:             c.cfg.instanceID,
		Service:        c.cfg.serviceName,
		Version:        c.cfg.serviceVersion,
		Address:        c.cfg.address,
		PollIntervalMs: c.cfg.pollInterval.Milliseconds(),
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

			// Trigger a re-register exactly once when we first cross 6 consecutive
			// failures. Manteion may have lost our registration (restart, eviction);
			// re-register is idempotent on the same instanceID. We don't re-fire on
			// every subsequent failure to avoid hammering a struggling server.
			if consecutiveFailures == 6 {
				c.wg.Go(func() { _ = c.register(c.pollCtx) })
			}
			return
		}

		if consecutiveFailures > 0 {
			c.logger.Info("manteion connection restored", "missed_polls", consecutiveFailures)
			// Force a full rule pull: manteion may have restarted or accumulated
			// changes we missed. Re-register with same instanceID (idempotent upsert).
			c.ruleVersion.Store(0)
			c.wg.Go(func() { _ = c.register(c.pollCtx) })
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

	req, err := c.newReq(ctx, http.MethodGet, fullURL, nil)
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
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return fmt.Errorf("read rules body: %w", err)
		}
		var payload struct {
			Version      uint64         `json:"version"`
			Rules        []CompiledRule `json:"rules"`
			ActiveFaults []FaultRequest `json:"active_faults"`
			FreezeCfg    *DelayRequest  `json:"freeze_cfg"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			return fmt.Errorf("decode rules: %w", err)
		}

		// Single application path: Apply decodes rules with the configured
		// NetworkResolver, reconciles active faults, and installs freeze config.
		// On error we keep stale state — manteion is alive, the issue is data.
		applyResp := RegisterResponse{
			Status:       "poll",
			Rules:        payload.Rules,
			ActiveFaults: payload.ActiveFaults,
			FreezeCfg:    payload.FreezeCfg,
		}
		if err := Apply(applyResp, c.targets); err != nil {
			c.logger.Error("apply failed, keeping stale state", "error", err)
			return nil
		}
		c.ruleVersion.Store(payload.Version)

		c.logger.Info("rules updated",
			"version", payload.Version,
			"rules_count", len(payload.Rules),
			"manual_faults", len(payload.ActiveFaults))
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
	req, err := c.newReq(ctx, http.MethodGet, sseURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

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
	sc.Buffer(make([]byte, 0, 8192), 1<<20) // grow up to 1 MiB per line

	var event string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			// Blank line = event boundary per SSE spec.
			if event == "rules_changed" {
				triggerPoll()
			}
			event = ""
		case strings.HasPrefix(line, ":"):
			// Comment / keepalive — ignore.
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			// data: lines ignored — we only care about the event name today.
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
	if _, err := rand.Read(b); err != nil {
		return host + "-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return host + "-" + hex.EncodeToString(b)
}
