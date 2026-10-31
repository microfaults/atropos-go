package instrument

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

// recoveryState is where the client stands on recovering a registration
// manteion has lost (a poll or a status report answered 404).
type recoveryState int

const (
	recoveryNone        recoveryState = iota // no recovery: manteion knows the instance, as far as the client can tell
	recoveryQueued                           // a status report's 404 claimed it; recoverRegistrations has not started it yet
	recoveryRegistering                      // the re-register is under way
	recoveryRegistered                       // the re-register went through, and no status report has landed since
)

// errUnknownInstance is a poll answered 404: manteion no longer knows this
// instance (its registration was lost), so the poll loop takes the recovery
// path (recoverRegistration).
var errUnknownInstance = errors.New("manteion does not know this instance (404)")

// ManteionClient manages the lifecycle of the SDK's connection to manteion:
// startup polling, registration, desired-state sync with its status report,
// and graceful shutdown.
//
// Lock order, outermost first: applyMu, the core's apply mutex
// (targets.ApplyMu), the replay-set puller's reconcileMu, the puller's p.mu.
// syncMu is a leaf, and statusMu is taken with no other lock held.
//   - applyMu is taken with no lock held: by applyDesired's locked half
//     (applyAndRecord), the 304 adoption, the watchdog's reap (reapSlots) and
//     an /admin/rules push's ETag clear (pushOverride). Under it, reconcile,
//     confirmSlots and reapStaleSlots take the core's apply mutex and release
//     it before they return. The push's apply takes the core's apply mutex
//     alone, and pushOverride runs after it.
//   - Under the core's apply mutex, reconcile hands the replay set to the
//     puller: Reconcile takes reconcileMu, then p.mu, and waits for a fetch
//     it supersedes to stop with p.mu released (that fetch's goroutine takes
//     only p.mu, and a superseded fetch calls no OnChange). The puller's
//     retry of a failed pull takes reconcileMu, then p.mu, with nothing else
//     held.
//   - statusMu serializes the status reports. sendStatus (applyDesired after
//     releasing applyMu, the puller's OnChange) and resendStatus (the poll
//     loop, retryStatus) take it with no other lock held; under it,
//     buildStatus takes syncMu and, after releasing it, p.mu (puller.Status).
//   - syncMu is taken under applyMu (the bookkeeping, adoptGeneration,
//     reapSlots) and under statusMu, and nothing is taken under it.
//
// No apply lock is held across network I/O: polls and registrations go out
// with no lock held, and applyDesired releases applyMu -- apply, the core's
// mutex -- before the drain reports and the status report go out. The two
// locks held across I/O exist to send reports one at a time: statusMu, and
// the drain tracker's drainMu, taken with no other lock held.
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
	// them when the poll loop recovers from failed polls and when manteion
	// has lost the registration (recoverRegistration). The writes of etag
	// alone clear it: reapSlots when the watchdog drops a slot the last apply
	// confirmed, pushOverride after an /admin/rules push. The next poll is
	// then unconditional, while the generation stays the one held. Every
	// writer but adoptGeneration also bumps applySeq.
	ruleVersion atomic.Uint64
	lastPollAt  atomic.Int64 // unix nanos; 0 = never polled
	status      atomic.Int32

	// applyMu serializes applyDesired's apply with its bookkeeping
	// (applyAndRecord), the 304 adoption, the watchdog's reap and a push's ETag
	// clear: an apply and the generation/ETag/report bookkeeping that follows
	// it are one unit, so a register goroutine, the poll loop, the watchdog
	// and an /admin/rules push never interleave.
	applyMu sync.Mutex
	// confirmed lists the active-fault slots the last successful apply that
	// reconciled slots confirmed, less any the watchdog has dropped since.
	// Every later answer that vouches for them -- a successful apply, a
	// no-document answer, a 304 -- re-confirms them (confirmSlots). Guarded
	// by applyMu.
	confirmed []string

	// syncMu guards what the status report snapshots, the apply sequence and
	// the recovery state.
	syncMu        sync.Mutex
	etag          string      // ETag of the document held, sent as If-None-Match; "" = none (writers: see ruleVersion)
	lastReport    applyReport // what the last apply could not hold
	statusPending bool        // the last status report failed; retryStatus re-sends it, and so does the poll loop before its next poll
	// applySeq counts the changes to the record of the document held:
	// applyDesired bumps it as it starts and again in the same syncMu section
	// that records what it applied, and forgetDocument and the ETag clears
	// (reapSlots, pushOverride) bump it with their writes. A poll snapshots it
	// with the generation and ETag it sends, and its 304 is adopted only while
	// applySeq is unchanged (adoptGeneration).
	applySeq uint64
	// recovery is the recovery of a lost registration (claimRecovery). It
	// holds from the 404 that claims it until a status report lands -- through
	// the re-register and after it -- so a manteion that goes on answering 404
	// is registered with once per loss, not in a loop.
	recovery recoveryState

	statusMu sync.Mutex // serializes sending: reports go out one at a time, in order
	// statusRetry wakes retryStatus once a status report has failed (capacity
	// 1: one wake-up stands for any number of failures). Nil in a client not
	// built by connectManteion, which has no retry: the poll loop's resend
	// is its only one.
	statusRetry chan struct{}
	// recoverWake wakes recoverRegistrations once a status report's 404 has
	// queued the recovery of the registration (capacity 1). Nil in a client
	// not built by connectManteion, which has no recovery goroutine.
	recoverWake chan struct{}

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
// the instance is always in a poll: manteion holds one whose version is the
// current generation and whose ETag matches until the generation changes or
// the hold elapses, answers a matching ETag with any other version (older,
// absent or zero, or ahead after a database reset) 304 at once with the
// generation to adopt, and anything else 200 at once; the next poll goes out
// as soon as the answer is handled -- at once, or once pollFloor has passed
// since the last one started. Without a hold the loop polls every
// pollInterval, the first time one interval after it starts (connectManteion's
// startup fetch has just run). A trigger (an SSE rules_changed nudge) ends any
// wait.
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
// manteion may have restarted or moved on meanwhile, so the instance
// re-converges on a full document through the register answer. A poll cut
// short by ctx is not a failure.
//
// A poll answered 404 means manteion no longer knows this instance: the loop
// takes the recovery path at once (recoverRegistration: forget the document
// held, register again, apply the answer) and resumes polling without a
// failure's backoff. Once per loss (claimRecovery): a 404 while the status
// path's recovery is queued or under way is no failure either -- the loop
// polls again once pollFloor has passed, and the restored path after it
// stays untaken -- but a 404 after a re-register that went through, while
// no report has landed since, is an ordinary failed poll.
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
		err := c.fetchRules(ctx)
		if errors.Is(err, errUnknownInstance) {
			switch c.claimRecovery(recoveryRegistering) {
			case recoveryNone: // claimed: recover now
				if err = c.recoverRegistration(ctx, "poll"); err == nil {
					consecutiveFailures = 0
					c.status.Store(int32(manteionConnected))
					delay = c.nextPollDelay(start)
					continue
				}
			case recoveryQueued, recoveryRegistering:
				// The status path is registering again: wait for it. Not a
				// failed poll, and nothing for the restored path to redo.
				consecutiveFailures = 0
				delay = max(0, pollFloor-time.Since(start))
				continue
			}
			// recoveryRegistered, or a re-register that failed: a failed poll.
		}
		if err != nil {
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
		delay = c.nextPollDelay(start)
	}
}

// nextPollDelay is the wait before the poll after one that started at start
// and was answered: with a hold, none once pollFloor has passed since start;
// without one, a pollInterval.
func (c *ManteionClient) nextPollDelay(start time.Time) time.Duration {
	if c.cfg.pollWait > 0 {
		return max(0, pollFloor-time.Since(start))
	}
	return c.cfg.pollInterval
}

// claimRecovery claims the recovery of a registration manteion has lost, as
// next (recoveryQueued for a status report's 404, which recoverRegistrations
// takes up; recoveryRegistering for the poll loop, which recovers at once),
// when none is claimed. It returns the state it found: recoveryNone means the
// claim is the caller's; any other is a recovery of the same loss, queued,
// under way or done -- one per loss, until a status report lands (postStatus)
// or the re-register fails.
func (c *ManteionClient) claimRecovery(next recoveryState) (found recoveryState) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	found = c.recovery
	if found == recoveryNone {
		c.recovery = next
	}
	return found
}

// recoverRegistration is the recovery path for a registration manteion has
// lost: forget the document held (forgetDocument), then register again,
// applying the answer in full and reporting it (register, applyDesired). A
// re-register that went through leaves the claim recoveryRegistered until a
// report lands; one that fails releases it, so a later 404 tries again.
func (c *ManteionClient) recoverRegistration(ctx context.Context, source string) error {
	c.logger.Warn("manteion does not know this instance; registering again", "source", source)
	c.forgetDocument()
	err := c.register(ctx)
	c.syncMu.Lock()
	switch {
	case err != nil:
		c.recovery = recoveryNone
	case c.recovery == recoveryRegistering: // the register's own report did not land
		c.recovery = recoveryRegistered
	}
	c.syncMu.Unlock()
	if err != nil {
		c.logger.Warn("re-register failed", "source", source, "error", err)
	}
	return err
}

// recoverRegistrations runs the recovery a status report's 404 queued
// (postStatus), until ctx is cancelled. A wake-up that finds the recovery no
// longer queued is dropped: a report landed after the 404 that queued it, so
// manteion knew the instance again, whatever 404 may have come since (the
// poll loop recovers its own).
func (c *ManteionClient) recoverRegistrations(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.recoverWake:
		}
		c.syncMu.Lock()
		queued := c.recovery == recoveryQueued
		if queued {
			c.recovery = recoveryRegistering
		}
		c.syncMu.Unlock()
		if queued {
			_ = c.recoverRegistration(ctx, "status report")
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
// confirmed are adopted without applying (unless the 304 is late: the record
// of the document held changed while the poll was out; see adoptGeneration),
// and the held slots re-confirmed. 200: the body is the desired state (1 MiB
// cap); its etag, falling back to the ETag header, is what applyDesired
// records. 404 is errUnknownInstance: manteion no longer knows this instance,
// and the poll loop recovers the registration. Other statuses are errors, for
// the poll loop's backoff.
func (c *ManteionClient) fetchRulesWait(ctx context.Context, wait time.Duration) error {
	c.syncMu.Lock()
	gen, etag, seq := c.ruleVersion.Load(), c.etag, c.applySeq
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
		c.adoptGeneration(resp.Header, seq)
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

	case http.StatusNotFound:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%w: %s", errUnknownInstance, body)

	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}
}

// adoptGeneration records what a 304 confirmed: X-Atropos-Generation becomes
// the generation held (ruleVersion and atropos_sync_generation), ETag the
// ETag held. An absent or unparsable header leaves the current value.
//
// sent is the apply sequence the poll snapshotted with the generation and
// ETag it was conditional on, and the 304 vouches only for that record. When
// the record changed while the poll was out -- a register applied a document
// (a newer generation of the same content keeps the ETag), the record was
// forgotten, or the watchdog's reap or an /admin/rules push cleared the ETag
// -- the 304 is late and adopts nothing: the generation never goes back, and
// the next poll is conditional on what the instance holds now. Called with
// applyMu held.
func (c *ManteionClient) adoptGeneration(h http.Header, sent uint64) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if c.applySeq != sent {
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
// and its 200 is applied in full; a poll already out comes back late
// (applySeq). The targets keep what they hold; only the record of which
// document that is goes.
func (c *ManteionClient) forgetDocument() {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.applySeq++
	c.ruleVersion.Store(0)
	c.etag = ""
	metrics.SyncGeneration.Set(0)
}

// applyDesired applies one desired-state document -- a 200 poll body or the
// register answer -- through the single apply path (scopeAll), records what
// the instance now holds and reports it: the generation (ruleVersion,
// atropos_sync_generation), the ETag the next poll sends back as
// If-None-Match, and the report of what could not be held. The apply and the
// bookkeeping run under applyMu (applyAndRecord); then, with no lock held,
// the drain reports of the recording phases the rule set ended go out, and
// the status report after them (statusMu and the snapshot built at send time
// keep the reports in order). source ("poll" | "register") labels the log
// lines.
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
// Every outcome bumps applySeq as the apply starts, and a successful apply
// bumps it again where it records the generation and ETag, so a poll out
// meanwhile -- or snapshotted while reconcile ran -- comes back late; every
// outcome sends the status report. A failed report is marked pending by
// sendStatus, never returned: applyDesired returns nil.
func (c *ManteionClient) applyDesired(ctx context.Context, doc wire.RuleSync, etag, source string) error {
	ended := c.applyAndRecord(doc, etag, source)
	c.targets.CacheDrain.Drain(ended)
	c.sendStatus(ctx)
	return nil
}

// applyAndRecord is applyDesired's half under applyMu: the apply (reconcile,
// which takes the core's apply mutex) and the bookkeeping that records what
// the instance now holds. It returns the recording phases whose drain
// reports are due, for applyDesired to send once applyMu is released.
func (c *ManteionClient) applyAndRecord(doc wire.RuleSync, etag, source string) []recordingPhaseKey {
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	c.syncMu.Lock()
	c.applySeq++
	c.syncMu.Unlock()

	// The predicate apply guards with, checked here first so the bookkeeping
	// never records the empty body as the state held.
	if doc.Etag == "" && doc.Rules == nil {
		c.logger.Warn("desired state: manteion sent no document; keeping the current state",
			"source", source, "generation", c.ruleVersion.Load(), "etag", c.currentEtag())
		c.confirmSlots()
		return nil
	}
	report, ended, err := reconcile(doc, c.targets, scopeAll)
	if err != nil {
		c.logger.Error("apply failed, keeping stale state", "source", source, "generation", doc.Version, "error", err)
		c.syncMu.Lock()
		c.lastReport = applyReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{err.Error()}}
		c.syncMu.Unlock()
		return nil // a hard error applies nothing and ends no phase
	}
	for _, e := range report.Errors {
		c.logger.Warn(source+": desired state not fully applied", "error", e)
	}
	if report.Confirmed != nil { // nil: the document left the slots alone
		c.confirmed = report.Confirmed
	}
	c.syncMu.Lock()
	// Bumped again with the record: a poll that snapshotted the record while
	// reconcile ran paired the early bump with the document held before, and
	// its 304 must come back late.
	c.applySeq++
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
	return ended
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
// leaves the confirmed set and the ETag is cleared (bumping applySeq): the
// next poll is unconditional (manteion answers it at once, without a hold)
// and its 200 re-applies the slot if it is still desired. A poll already out
// comes back as a late 304, which adopts nothing (adoptGeneration). Dropping
// a slot the document never named (an /admin/fault slot) changes neither.
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
	c.applySeq++
	c.etag = ""
	c.syncMu.Unlock()
	c.logger.Warn("watchdog: dropped a slot of the desired state; the next poll fetches the desired state in full")
}

// pushOverride records an /admin/rules push: the rules the instance holds are
// no longer the document its ETag names, so the ETag is cleared (bumping
// applySeq), as reapSlots does. The next poll is unconditional -- manteion
// answers it at once -- and its 200 re-applies the desired state over the
// pushed set; a poll already out comes back as a late 304 and adopts
// nothing. The generation held stays, and no status report goes out. It takes
// applyMu, so it comes after any apply in progress and its bookkeeping, which
// cannot then record an ETag over the pushed set. Nil-safe: offline there is
// no ETag to clear.
func (c *ManteionClient) pushOverride() {
	if c == nil {
		return
	}
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	c.syncMu.Lock()
	c.applySeq++
	c.etag = ""
	c.syncMu.Unlock()
	c.logger.Info("rules pushed through /admin/rules; the next poll re-applies the desired state")
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
// one lost report. A 404 means manteion no longer knows this instance: the
// report stays pending, and the recovery of the registration is queued for
// recoverRegistrations (claimRecovery), whose register sends a fresh report.
// A 2xx also ends a recovery: manteion knows the instance again. Serialized on
// statusMu: reports go out one at a time, each built when it is sent, so the
// last one manteion receives is the newest.
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
	if resp.StatusCode == http.StatusNotFound {
		c.statusFailed("status report rejected: manteion does not know this instance", fmt.Errorf("status %d", resp.StatusCode))
		if c.claimRecovery(recoveryQueued) == recoveryNone {
			select {
			case c.recoverWake <- struct{}{}:
			default:
			}
		}
		return
	}
	if resp.StatusCode/100 != 2 {
		c.statusFailed("status report rejected", fmt.Errorf("status %d", resp.StatusCode))
		return
	}
	c.syncMu.Lock()
	c.statusPending = false
	c.recovery = recoveryNone // manteion knows this instance
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
