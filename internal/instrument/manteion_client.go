package instrument

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// manteionStatus represents the SDK's connectivity state with manteion.
type manteionStatus int32

const (
	manteionDisconnected manteionStatus = iota // never connected (or offline)
	manteionConnected                          // poll succeeding normally
	manteionDegraded                           // recent poll failures; using stale rules
)

// ManteionClient manages the lifecycle of the SDK's connection to manteion:
// startup polling, registration, desired-state sync with its status report,
// and graceful shutdown.
//
// Lock order: applyMu comes first. While it is held, apply, confirmSlots and
// the watchdog's reap (reapSlots) take the core's apply mutex
// (targets.ApplyMu) and release it before they return, and sendStatus takes
// statusMu, under which buildStatus takes syncMu and, after releasing it, the
// replay-set puller's lock (puller.Status). syncMu is a leaf. The
// /admin/rules push takes only the core's apply mutex; the puller calls
// OnChange (sendStatus), and the poll loop and retryStatus call resendStatus
// (statusMu), with no lock held. Nothing takes applyMu while holding any of
// the others.
type ManteionClient struct {
	cfg        manteionConfig
	httpClient *http.Client // short-lived calls: init, register, status, deregister, a poll without a hold
	pollClient *http.Client // a held poll (wait > 0): its timeout outlasts the hold; nil = httpClient
	sseClient  *http.Client // no Timeout — SSE connections are indefinitely long-lived
	targets    applyTargets
	logger     *slog.Logger

	// ruleVersion is the generation of the desired state the instance holds
	// (spec §2: the poll's version parameter, the status report's
	// applied_generation, HealthStatus.RuleVersion). It moves together with
	// etag and the atropos_sync_generation gauge, all three written under
	// syncMu: applyDesired sets them after a successful apply,
	// adoptGeneration after a 304 that is not late, and forgetDocument zeroes
	// them when the poll loop recovers from failed polls. The one write of
	// etag alone is reapSlots clearing it when the watchdog drops a slot the
	// last apply confirmed: the next poll is then unconditional, while the
	// generation stays the one held.
	ruleVersion atomic.Uint64
	lastPollAt  atomic.Int64 // unix nanos; 0 = never polled
	status      atomic.Int32

	// applyMu serializes applyDesired, the 304 adoption and the watchdog's
	// reap: an apply and the generation/ETag/report bookkeeping that follows
	// it are one unit, so a register goroutine, the poll loop and the
	// watchdog never interleave.
	applyMu sync.Mutex
	// confirmed lists the active-fault slots the last successful apply that
	// reconciled slots confirmed, less any the watchdog has dropped since.
	// Every later answer that vouches for them -- a successful apply, a
	// no-document answer, a 304 -- re-confirms them (confirmSlots). Guarded
	// by applyMu.
	confirmed []string

	// syncMu guards what the status report snapshots.
	syncMu        sync.Mutex
	etag          string      // ETag of the document held, sent as If-None-Match; "" = none (writers: see ruleVersion)
	lastReport    applyReport // what the last apply could not hold
	statusPending bool        // the last status report failed; retryStatus re-sends it, and so does the poll loop before its next poll

	statusMu sync.Mutex // serializes sending: reports go out one at a time, in order
	// statusRetry wakes retryStatus once a status report has failed (capacity
	// 1: one wake-up stands for any number of failures). Nil in a client not
	// built by connectManteion, which has no retry: the poll loop's resend
	// is its only one.
	statusRetry chan struct{}

	// puller converges the cache box's replay set; nil when this host has no
	// cache box (no replay-set support). Closed by Close.
	puller *replaySetPuller

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
func (c *ManteionClient) Status() manteionStatus {
	if c == nil {
		return manteionDisconnected
	}
	return manteionStatus(c.status.Load())
}

// Close cancels the poll loop, closes the replay-set puller (ending a fetch
// in flight and the status reports it would send), waits for the client's
// goroutines to exit, and sends a best-effort deregister to manteion. Safe to
// call on a nil receiver (offline mode). Always returns nil today
// (best-effort); reserved for future fatal-error reporting.
func (c *ManteionClient) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.puller.Close()
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

// register POSTs the registration, then applies the desired state manteion
// answers with -- the same document a 200 poll carries -- through
// applyDesired.
func (c *ManteionClient) register(ctx context.Context) error {
	routes := c.cfg.routes
	if len(routes) == 0 {
		routes = nil // omit the field entirely from the payload
	}
	// manteion counts an instance stale after twice its advertised poll
	// interval and dead after five times; a held poll comes back about once
	// per hold, so the cadence advertised is the longer of the two.
	cadence := max(c.cfg.pollInterval, c.cfg.pollWait)
	resp, err := registerWith(ctx, c.httpClient, c.cfg.url, wire.RegisterRequest{
		ID:             c.cfg.instanceID,
		Service:        c.cfg.serviceName,
		Version:        c.cfg.serviceVersion,
		Address:        c.cfg.address,
		PollIntervalMs: cadence.Milliseconds(),
		Routes:         routes,
		Host:           c.cfg.host,
		Capabilities:   c.cfg.capabilities,
	})
	if err != nil {
		return err
	}
	return c.applyDesired(ctx, resp.RuleSync, resp.Etag, "register")
}

// pollFloor is the shortest time between the starts of two polls with a
// long-poll hold: a server that answers a held poll at once (one that
// ignores wait) is re-polled no faster than this. Tests set it to 0.
var pollFloor = time.Second

// pollLoopWithTrigger runs until ctx is cancelled, keeping the instance on
// manteion's desired state (spec §3). With a long-poll hold (pollWait > 0)
// the instance is always in a poll: manteion holds each until the desired
// state changes or the hold elapses, and the next poll goes out as soon as
// the answer is handled -- at once, or once pollFloor has passed since the
// last one started. Without a hold the loop polls every pollInterval, the
// first time one interval after it starts (connectManteion's startup fetch
// has just run). A trigger (an SSE rules_changed nudge) ends any wait.
//
// Before each poll a status report that did not reach manteion is re-sent
// (resendStatus), so manteion's convergence barrier never starves on one lost
// report; retryStatus re-sends it sooner, since a hold can keep the next poll
// away for up to pollWait. A failed poll keeps the stale state, marks the
// client degraded and backs off pollInterval·2^min(failures,6), at most
// 60 s. The sixth consecutive failure re-registers once -- manteion may have
// lost the registration (restart, eviction); register is an idempotent
// upsert on the instance id -- and later failures do not, so a struggling
// server is not hammered. The first poll that succeeds after failures
// forgets the document held (forgetDocument) and only then re-registers:
// manteion may have restarted or moved on meanwhile, so the next poll is a
// full pull. A poll cut short by ctx is not a failure.
func (c *ManteionClient) pollLoopWithTrigger(ctx context.Context, trigger <-chan struct{}) {
	var delay time.Duration // with a hold, straight into the held poll
	if c.cfg.pollWait == 0 {
		delay = c.cfg.pollInterval
	}
	consecutiveFailures := 0
	for {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			case <-trigger:
			}
		}
		c.resendStatus(ctx)
		start := time.Now()
		if err := c.fetchRules(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			consecutiveFailures++
			c.status.Store(int32(manteionDegraded))
			delay = min(c.cfg.pollInterval*time.Duration(1<<min(consecutiveFailures, 6)), 60*time.Second)
			c.logger.Warn("manteion poll failed, continuing with stale rules",
				"failures", consecutiveFailures,
				"next_retry", delay,
				"error", err,
			)
			if consecutiveFailures == 6 {
				c.wg.Go(func() { _ = c.register(c.pollCtx) })
			}
			continue
		}

		if consecutiveFailures > 0 {
			c.logger.Info("manteion connection restored", "missed_polls", consecutiveFailures)
			c.forgetDocument()
			c.wg.Go(func() { _ = c.register(c.pollCtx) })
			consecutiveFailures = 0
		}
		c.lastPollAt.Store(time.Now().UnixNano())
		c.status.Store(int32(manteionConnected))
		if c.cfg.pollWait > 0 {
			delay = max(0, pollFloor-time.Since(start))
		} else {
			delay = c.cfg.pollInterval
		}
	}
}

// fetchRules polls manteion for the desired state with the configured
// long-poll hold (cfg.pollWait; 0 = an immediate answer).
func (c *ManteionClient) fetchRules(ctx context.Context) error {
	return c.fetchRulesWait(ctx, c.cfg.pollWait)
}

// fetchRulesWait calls GET /api/v1/sdk/rules conditionally on what the
// instance holds (spec §2): version is the generation held, If-None-Match the
// ETag held (sent only when there is one: a document without an ETag makes
// the next poll unconditional, never a quoted empty string), and wait asks
// manteion to hold the request up to that long, in whole seconds rounded up
// (sent only when wait > 0). A held poll goes through pollClient, whose
// timeout outlasts the hold; one without a hold through httpClient.
//
// 304: the document is provably unchanged; the generation and ETag manteion
// confirmed are adopted without applying (unless the 304 is late: see
// adoptGeneration), and the held slots re-confirmed. 200: the body is the
// desired state (1 MiB cap); its etag, falling back to the ETag header, is
// what applyDesired records. Other statuses are errors, for the poll loop's
// backoff.
func (c *ManteionClient) fetchRulesWait(ctx context.Context, wait time.Duration) error {
	c.syncMu.Lock()
	gen, etag := c.ruleVersion.Load(), c.etag
	c.syncMu.Unlock()

	q := url.Values{
		"service":     {c.cfg.serviceName},
		"instance_id": {c.cfg.instanceID},
		"version":     {strconv.FormatUint(gen, 10)},
	}
	if wait > 0 {
		q.Set("wait", strconv.FormatInt(int64((wait+time.Second-1)/time.Second), 10))
	}
	fullURL := c.cfg.url + "/api/v1/sdk/rules?" + q.Encode()

	req, err := c.newReq(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return fmt.Errorf("build rules request: %w", err)
	}
	if etag != "" {
		req.Header.Set("If-None-Match", `"`+etag+`"`)
	}
	hc := c.httpClient
	if wait > 0 && c.pollClient != nil {
		hc = c.pollClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("poll failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusNotModified:
		c.applyMu.Lock()
		defer c.applyMu.Unlock()
		c.adoptGeneration(resp.Header, etag)
		c.confirmSlots()
		return nil

	case http.StatusOK:
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		if err != nil {
			return fmt.Errorf("read rules body: %w", err)
		}
		var payload wire.RuleSync
		if err := json.Unmarshal(body, &payload); err != nil {
			return fmt.Errorf("decode rules: %w", err)
		}
		etag := payload.Etag
		if etag == "" {
			etag = strings.Trim(resp.Header.Get("ETag"), `"`)
		}
		return c.applyDesired(ctx, payload, etag, "poll")

	case http.StatusServiceUnavailable:
		return fmt.Errorf("manteion not ready (503)")

	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
}

// adoptGeneration records what a 304 confirmed: X-Atropos-Generation becomes
// the generation held (ruleVersion and atropos_sync_generation), ETag the
// ETag held. An absent or unparsable header leaves the current value.
//
// sent is the ETag the poll was conditional on ("" = none), and the 304
// vouches only for that document. When the ETag held is no longer sent -- a
// register applied a newer document, or the watchdog dropped a confirmed
// slot and cleared the ETag, while the poll was out -- the 304 is late and
// adopts nothing: the next poll stays conditional on what the instance holds
// now. Called with applyMu held.
func (c *ManteionClient) adoptGeneration(h http.Header, sent string) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.etag != sent {
		return
	}
	if gen, err := strconv.ParseUint(h.Get("X-Atropos-Generation"), 10, 64); err == nil {
		c.ruleVersion.Store(gen)
		metrics.SyncGeneration.Set(float64(gen))
	}
	if etag := strings.Trim(h.Get("ETag"), `"`); etag != "" {
		c.etag = etag
	}
}

// forgetDocument drops the record of the document the instance holds -- the
// generation, the ETag and the atropos_sync_generation gauge, together under
// syncMu, never one without the others -- so the next poll is unconditional
// and its 200 is applied in full. The targets keep what they hold; only the
// record of which document that is goes.
func (c *ManteionClient) forgetDocument() {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.ruleVersion.Store(0)
	c.etag = ""
	metrics.SyncGeneration.Set(0)
}

// applyDesired applies one desired-state document -- a 200 poll body or the
// register answer -- through the single apply path (scopeAll), records what
// the instance now holds and reports it: the generation (ruleVersion,
// atropos_sync_generation), the ETag the next poll sends back as
// If-None-Match, and the report of what could not be held; then the status
// report goes out. source ("poll" | "register") labels the log lines.
//
// A no-document body -- neither an etag nor rules: manteion could not
// materialize the desired state (spec §2 Register) -- changes nothing: the
// generation, ETag and report stay what the instance holds, so the next poll
// stays conditional on it, and the status report repeats them. A hard error
// applies nothing either (apply checks before it touches a target), so the
// generation and ETag stay; the error becomes the report -- manteion is
// alive, the problem is the data. A successful apply and a no-document body
// re-confirm the held slots (confirmSlots), as a 304 does; a hard error does
// not: a document the instance cannot hold vouches for nothing, so the held
// slots age out under the watchdog unless a later answer confirms them.
// Every outcome sends the status report; a failed report is marked pending
// by sendStatus, never returned: applyDesired returns nil.
func (c *ManteionClient) applyDesired(ctx context.Context, doc wire.RuleSync, etag, source string) error {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()

	// The predicate apply guards with, checked here first so the bookkeeping
	// never records the empty body as the state held.
	if doc.Etag == "" && doc.Rules == nil {
		c.logger.Warn("desired state: manteion sent no document; keeping the current state",
			"source", source, "generation", c.ruleVersion.Load(), "etag", c.currentEtag())
		c.confirmSlots()
	} else if report, err := apply(doc, c.targets, scopeAll); err != nil {
		c.logger.Error("apply failed, keeping stale state", "source", source, "generation", doc.Version, "error", err)
		c.syncMu.Lock()
		c.lastReport = applyReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{err.Error()}}
		c.syncMu.Unlock()
	} else {
		for _, e := range report.Errors {
			c.logger.Warn(source+": desired state not fully applied", "error", e)
		}
		if report.Confirmed != nil { // nil: the document left the slots alone
			c.confirmed = report.Confirmed
		}
		c.syncMu.Lock()
		c.ruleVersion.Store(doc.Version)
		c.etag = etag
		c.lastReport = report
		metrics.SyncGeneration.Set(float64(doc.Version))
		c.syncMu.Unlock()
		c.logger.Info("rules updated",
			"source", source,
			"version", doc.Version,
			"generation", doc.Version,
			"etag", etag,
			"rules_count", len(doc.Rules),
			"manual_faults", len(doc.ActiveFaults),
			"unsupported", len(report.Unsupported))
		c.confirmSlots()
	}
	c.sendStatus(ctx)
	return nil
}

// confirmSlots re-confirms the active-fault slots the last successful apply
// that reconciled slots confirmed. apply confirms the slots it applies, but
// only on a 200 that carries them; a 304 vouches for the same slots (as does
// a 200 that applied nothing or left the slots alone, and a no-document
// answer), so without this a steady state of 304s would let the watchdog reap
// desired slots after its grace. Slots the document never named (an
// /admin/fault slot) are not confirmed and still age out. Called with applyMu
// held; takes the core's apply mutex like the watchdog's reap, so a reap
// never clears a slot between its staleness check and this refresh.
func (c *ManteionClient) confirmSlots() {
	if c.targets.DemoEval == nil || len(c.confirmed) == 0 {
		return
	}
	if mu := c.targets.ApplyMu; mu != nil {
		mu.Lock()
		defer mu.Unlock()
	}
	for _, id := range c.confirmed {
		c.targets.DemoEval.Confirm(id)
	}
}

// watchdogGrace is how long a fault slot survives unconfirmed before the
// watchdog drops it: three poll cadences, never less than 30 s. The cadence
// is the longer of the poll interval and the long-poll hold, since a held
// poll is answered -- and the slots re-confirmed -- about once per hold; a
// grace of one hold could drop a desired slot between two held 304s.
func watchdogGrace(pollInterval, pollWait time.Duration) time.Duration {
	return max(3*max(pollInterval, pollWait), 30*time.Second)
}

// watchSlots is the fault-slot watchdog: every second until ctx is
// cancelled, reapSlots drops the slots not confirmed within grace. It is the
// only guard against zombie faults -- a slot with duration_ms = 0 (infinite)
// whose manteion has crashed -- and it keeps its own cadence, apart from the
// poll loop's, so it still reaps while polls fail.
func (c *ManteionClient) watchSlots(ctx context.Context, grace time.Duration) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.reapSlots(grace)
		}
	}
}

// reapSlots is one watchdog pass, under applyMu: reapStaleSlots drops the
// slots not confirmed within grace. Dropping a slot the last apply confirmed
// means the instance no longer holds the document its ETag names, so the slot
// leaves the confirmed set and the ETag is cleared: the next poll is
// unconditional (manteion answers it at once, without a hold) and its 200
// re-applies the slot if it is still desired. A poll already out comes back
// as a late 304, which adopts nothing (adoptGeneration). Dropping a slot the
// document never named (an /admin/fault slot) changes neither.
func (c *ManteionClient) reapSlots(grace time.Duration) {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	reaped := reapStaleSlots(c.targets.DemoEval, c.targets.Registry, c.targets.ApplyMu, grace, c.logger)
	kept := make([]string, 0, len(c.confirmed))
	for _, id := range c.confirmed {
		if !slices.Contains(reaped, id) {
			kept = append(kept, id)
		}
	}
	if len(kept) == len(c.confirmed) {
		return
	}
	c.confirmed = kept
	c.syncMu.Lock()
	c.etag = ""
	c.syncMu.Unlock()
	c.logger.Warn("watchdog: dropped a slot of the desired state; the next poll fetches the desired state in full")
}

// statusTimeout bounds one status report POST.
const statusTimeout = 5 * time.Second

// buildStatus snapshots what the instance holds for the status report (spec
// §2, a full snapshot): the generation and ETag held, copies of the last
// report's lists -- never nil, so an empty list is sent as [] and clears
// manteion's previous report -- extended with the replay-set puller's failure
// reason, and the replay set the puller holds (nil when none).
func (c *ManteionClient) buildStatus() wire.StatusReport {
	c.syncMu.Lock()
	s := wire.StatusReport{
		InstanceID:        c.cfg.instanceID,
		Service:           c.cfg.serviceName,
		AppliedGeneration: c.ruleVersion.Load(),
		Etag:              c.etag,
		Unsupported:       append([]wire.UnsupportedRule{}, c.lastReport.Unsupported...),
		Errors:            append([]string{}, c.lastReport.Errors...),
	}
	c.syncMu.Unlock()
	replaySet, errMsg := c.puller.Status()
	s.ReplaySet = replaySet
	if errMsg != "" {
		s.Errors = append(s.Errors, errMsg)
	}
	return s
}

// sendStatus POSTs the current status snapshot to /api/v1/sdk/status, with
// a statusTimeout deadline on top of ctx. Any 2xx clears statusPending; a
// transport error or another status logs a warning, sets it and wakes
// retryStatus, which re-sends after a short backoff, as the poll loop does
// before its next poll -- manteion's convergence barrier must not starve on
// one lost report. Serialized on statusMu: reports go out one at a time, each
// built when it is sent, so the last one manteion receives is the newest.
func (c *ManteionClient) sendStatus(ctx context.Context) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.postStatus(ctx)
}

// resendStatus re-sends the status report if one is pending. The flag is
// checked again once statusMu is held, so a report that landed while this
// waited for the lock -- the retry's, the loop's, a later apply's -- is not
// sent twice.
func (c *ManteionClient) resendStatus(ctx context.Context) {
	if !c.statusIsPending() {
		return
	}
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	if c.statusIsPending() {
		c.postStatus(ctx)
	}
}

// postStatus sends one status report (see sendStatus). Called with statusMu
// held.
func (c *ManteionClient) postStatus(ctx context.Context) {
	status := c.buildStatus()
	body, err := json.Marshal(status)
	if err != nil {
		c.statusFailed("marshal status report", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, statusTimeout)
	defer cancel()
	req, err := c.newReq(ctx, http.MethodPost, c.cfg.url+"/api/v1/sdk/status", bytes.NewReader(body))
	if err != nil {
		c.statusFailed("build status request", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.statusFailed("send status report", err)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		c.statusFailed("status report rejected", fmt.Errorf("status %d", resp.StatusCode))
		return
	}
	c.syncMu.Lock()
	c.statusPending = false
	c.syncMu.Unlock()
}

// statusFailed logs a status report that did not reach manteion, marks it
// pending for a re-send and wakes retryStatus (nothing to do when a wake-up
// is already queued, or when the client has no retry).
func (c *ManteionClient) statusFailed(what string, err error) {
	c.logger.Warn("status report not delivered, will re-send", "step", what, "error", err)
	c.syncMu.Lock()
	c.statusPending = true
	c.syncMu.Unlock()
	select {
	case c.statusRetry <- struct{}{}:
	default:
	}
}

// statusRetryBase is retryStatus's first wait after a failed status report;
// each failed re-send doubles it, up to the poll wait. Tests shrink it.
var statusRetryBase = 2 * time.Second

// retryStatus re-sends a status report that did not reach manteion, on its
// own, until ctx is cancelled. The poll loop re-sends before each poll, but a
// held poll keeps the next one away for up to pollWait, while manteion's
// convergence barrier (MANTEION_CONVERGE_TIMEOUT, 30 s by default) must not
// starve on one lost report -- above all the replay set's committed, which
// the puller sends in the middle of a hold. Woken by statusFailed, it waits
// statusRetryBase, doubling up to the poll wait (the poll interval without a
// hold), and re-sends while the report is pending. The first report that
// lands -- this retry's, the loop's or a later apply's -- ends the run; the
// next failure starts again from statusRetryBase. One goroutine, and every
// send under statusMu: retries never overlap each other or another report.
func (c *ManteionClient) retryStatus(ctx context.Context) {
	limit := c.cfg.pollWait
	if limit <= 0 {
		limit = c.cfg.pollInterval
	}
	limit = max(limit, statusRetryBase)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.statusRetry:
		}
		for wait := statusRetryBase; c.statusIsPending(); wait = min(2*wait, limit) {
			if !sleepCtx(ctx, wait) {
				return
			}
			c.resendStatus(ctx)
		}
	}
}

// currentEtag is the ETag held ("" = none): the next poll's If-None-Match.
func (c *ManteionClient) currentEtag() string {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.etag
}

// statusIsPending reports whether the last status report failed to reach
// manteion and must be re-sent.
func (c *ManteionClient) statusIsPending() bool {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	return c.statusPending
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
