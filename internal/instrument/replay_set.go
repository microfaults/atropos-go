package instrument

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// replaySetChunkBytes is the size of one Range request against a replay-set
// artifact: the instance fetches in ranges of at most 4 MiB (desired-state
// spec §2). A var so tests can shrink it until NDJSON lines straddle chunks.
var replaySetChunkBytes int64 = 4 << 20

const (
	// replaySetMaxAttempts bounds the requests made for one chunk. A request
	// that cannot be built or sent, a 5xx, a 408 or a 429 is retried from the
	// same offset after pushBackoff(attempt); the attempt count resets after
	// every chunk and for every fetch.
	replaySetMaxAttempts = 5
	// replaySetChunkTimeout bounds one chunk's request, body included. The
	// client has no Timeout of its own: a whole artifact may take longer than
	// any one chunk.
	replaySetChunkTimeout = 30 * time.Second
)

// replaySetRetryBase and replaySetRetryMax pace the retry of a failed pull
// while its set stays desired: the first retry comes replaySetRetryBase after
// the failure, each further one twice as long after the one before, at most
// replaySetRetryMax, every wait spread by ±20 % (jitterRetry). Vars so tests
// can shrink them; newReplaySetPuller copies them.
var (
	replaySetRetryBase = 5 * time.Second
	replaySetRetryMax  = time.Minute
)

// replaySetPullerConfig configures a replaySetPuller.
type replaySetPullerConfig struct {
	// BaseURL is the manteion base URL. A relative ReplaySetRef.URL is
	// appended to it; an absolute one is used as is.
	BaseURL string
	// Client performs the artifact requests. Nil means a client with no
	// Timeout: each chunk's deadline comes from its context.
	Client *http.Client
	// NewRequest builds each artifact request (manteion auth goes here). Nil
	// means http.NewRequestWithContext with no body.
	NewRequest func(ctx context.Context, method, url string) (*http.Request, error)
	// CacheBox receives the fetched entries through its preload staging. A nil
	// CacheBox leaves nothing to install into: newReplaySetPuller returns nil.
	CacheBox *cachebox.CacheBox
	Logger   *slog.Logger // nil means slog.Default()
	// OnChange, if set, is called after every transition the puller makes on
	// its own -- to committed or failed when a fetch ends, and to fetching
	// when a retry starts one -- from the goroutine that made it, once Status
	// reports the new state and with no lock held. It must not call Close,
	// which waits for that goroutine.
	OnChange func()
}

// replaySetPuller converges the cache box's installed replay set on the one
// manteion desires (desired-state spec §2-§3). Reconcile starts a fetch when
// the desired id differs from the committed one; the fetch pulls the artifact
// in Range chunks, stages the entries through the cache box's preload path
// and commits only when the entry count and the W5 digest match; Status
// reports the state for the status report. A pull that fails is pulled again
// on a timer while its set stays desired (armRetryLocked), since manteion
// answers an unchanged document 304 and never re-sends the set.
//
// At most one fetch runs at a time. Every fetch stages into the cache box's
// single preload slot, so a fetch is cancelled and waited for before the next
// one begins.
//
// A nil *replaySetPuller means "no replay-set support": Reconcile, Status and
// Close are no-ops on it.
type replaySetPuller struct {
	baseURL    string
	client     *http.Client
	newRequest func(ctx context.Context, method, url string) (*http.Request, error)
	cb         *cachebox.CacheBox
	logger     *slog.Logger
	onChange   func()

	// The retry of a failed pull waits retryDelay: retryBase, doubling up to
	// retryMax, spread by jitter. Set at construction.
	retryBase, retryMax time.Duration
	jitter              func(time.Duration) time.Duration

	ctx  context.Context // parent of every fetch and retry; Close cancels it
	stop context.CancelFunc

	reconcileMu sync.Mutex // serializes Reconcile and a retry's start: one supersede-then-start at a time

	mu          sync.Mutex
	committedID string       // id of the set the last successful commit installed; "" = none
	state       string       // "" (no set desired) | fetching | committed | failed
	id          string       // the desired set's id, which state is about
	errMsg      string       // the failure reason; non-empty only in state failed
	current     *replayFetch // the fetch whose outcome sets state; nil when none does
	retry       *replayRetry // the retry armed for the desired set's failed pull; nil when none is
	failures    int          // consecutive failed pulls of the desired set: the retry's backoff
	closed      bool
	wg          sync.WaitGroup // fetch and retry goroutines, waited for by Close
}

// replayFetch is one fetch goroutine.
type replayFetch struct {
	id     string
	scope  cachebox.PhasePair
	cancel context.CancelFunc
	// done is closed once the fetch has stopped touching the preload staging
	// and a commit it made is recorded in committedID.
	done chan struct{}
}

// is reports whether f fetches id under scope; false for a nil f.
func (f *replayFetch) is(id string, scope cachebox.PhasePair) bool {
	return f != nil && f.id == id && f.scope == scope
}

// replayRetry is the retry armed for a failed pull: its goroutine pulls id
// under scope again once its wait is over, unless cancel drops it first.
type replayRetry struct {
	id     string
	scope  cachebox.PhasePair
	cancel context.CancelFunc
}

// is reports whether r retries id under scope; false for a nil r.
func (r *replayRetry) is(id string, scope cachebox.PhasePair) bool {
	return r != nil && r.id == id && r.scope == scope
}

// newReplaySetPuller builds a puller from cfg, defaulting the unset fields.
// It returns nil, the no-replay-set-support puller, when cfg.CacheBox is nil.
func newReplaySetPuller(cfg replaySetPullerConfig) *replaySetPuller {
	if cfg.CacheBox == nil {
		return nil
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{}
	}
	if cfg.NewRequest == nil {
		cfg.NewRequest = func(ctx context.Context, method, target string) (*http.Request, error) {
			return http.NewRequestWithContext(ctx, method, target, nil)
		}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, stop := context.WithCancel(context.Background())
	return &replaySetPuller{
		baseURL:    cfg.BaseURL,
		client:     cfg.Client,
		newRequest: cfg.NewRequest,
		cb:         cfg.CacheBox,
		logger:     cfg.Logger,
		onChange:   cfg.OnChange,
		retryBase:  replaySetRetryBase,
		retryMax:   replaySetRetryMax,
		jitter:     jitterRetry,
		ctx:        ctx,
		stop:       stop,
	}
}

// Reconcile makes ref, installed under scope, the desired replay set. It does
// not wait for a fetch, only for a superseded one to stop:
//   - ref nil: no set is desired. Any fetch is cancelled, an armed retry
//     dropped, the installed set is cleared and Status reports nothing.
//   - ref already being fetched under scope, or failed under scope with its
//     retry armed: no-op; the retry keeps its schedule.
//   - any other fetch in flight is cancelled and waited for first, since the
//     next fetch stages into the same preload slot. Left running, it could
//     also install a set nobody desires any more. A retry armed for another
//     set (or another scope) is dropped, and the backoff starts over.
//   - ref.ID is the committed set and it is still installed under scope:
//     nothing to fetch; the state names ref.ID as committed again (a newer
//     set may have failed or been cancelled in between).
//   - otherwise the state turns fetching and a fetch starts in a goroutine.
//
// Reconcile after Close does nothing.
func (p *replaySetPuller) Reconcile(ref *wire.ReplaySetRef, scope cachebox.PhasePair) {
	if p == nil {
		return
	}
	p.reconcileMu.Lock()
	defer p.reconcileMu.Unlock()

	p.mu.Lock()
	old := p.current
	if p.closed || (ref != nil && (old.is(ref.ID, scope) || p.retry.is(ref.ID, scope))) {
		p.mu.Unlock()
		return
	}
	p.current = nil // detached: its outcome no longer sets the state, arms a retry or calls OnChange
	p.dropRetryLocked()
	p.failures = 0
	p.mu.Unlock()
	if old != nil {
		old.cancel()
		<-old.done
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	switch {
	case ref == nil:
		cleared, prev := p.cb.ReplaySetPhaseKey() != "", p.committedID
		if cleared {
			p.cb.ClearReplaySet()
		}
		p.committedID = ""
		p.setStateLocked("", "", "")
		p.mu.Unlock()
		if cleared {
			p.logger.Info("replay set cleared: none desired", "id", prev)
		}
	case p.committedID != "" && ref.ID == p.committedID &&
		p.cb.ReplaySetPhaseKey() == cachebox.PhaseKey(scope.ExperimentID, scope.PhaseID):
		p.setStateLocked(ref.ID, wire.ReplaySetCommitted, "")
		p.mu.Unlock()
	default:
		p.startLocked(*ref, scope)
		p.mu.Unlock()
		p.logger.Info("replay set fetch started", "id", ref.ID,
			"experiment_id", scope.ExperimentID, "phase_id", scope.PhaseID, "size_bytes", ref.SizeBytes)
	}
}

// startLocked starts a fetch of ref under scope: it becomes the current
// fetch, the state turns fetching and the fetch runs in its own goroutine.
// p.mu must be held.
func (p *replaySetPuller) startLocked(ref wire.ReplaySetRef, scope cachebox.PhasePair) {
	ctx, cancel := context.WithCancel(p.ctx)
	f := &replayFetch{id: ref.ID, scope: scope, cancel: cancel, done: make(chan struct{})}
	p.current = f
	p.setStateLocked(ref.ID, wire.ReplaySetFetching, "")
	p.wg.Add(1)
	go p.run(ctx, f, ref, scope)
}

// Status reports the desired set's state for the status report: nil when no
// set is desired. errMsg is the failure reason, non-empty only in state failed.
func (p *replaySetPuller) Status() (status *wire.ReplaySetStatus, errMsg string) {
	if p == nil {
		return nil, ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == "" {
		return nil, ""
	}
	return &wire.ReplaySetStatus{ID: p.id, State: p.state}, p.errMsg
}

// Close cancels any fetch, drops an armed retry and waits for their
// goroutines. A fetch cancelled by Close is not reported (no failed state, no
// OnChange): shutdown is not a failure. Safe to call more than once.
func (p *replaySetPuller) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.closed = true
	p.current = nil
	p.dropRetryLocked()
	p.mu.Unlock()
	p.stop()
	p.wg.Wait()
}

// dropRetryLocked cancels the armed retry, if any: its goroutine ends without
// pulling. p.mu must be held.
func (p *replaySetPuller) dropRetryLocked() {
	if p.retry != nil {
		p.retry.cancel()
		p.retry = nil
	}
}

// setStateLocked records the state reported for id and mirrors it into the
// atropos_replay_set_state gauge. p.mu must be held.
func (p *replaySetPuller) setStateLocked(id, state, errMsg string) {
	p.id, p.state, p.errMsg = id, state, errMsg
	metrics.SetReplaySetState(state)
}

// run executes one fetch and records its outcome. A fetch that committed is
// recorded in committedID even when superseded, because the cache box now
// holds its set; the state, the gauge, the retry and OnChange belong to the
// fetch that is still current. A current fetch that fails arms the retry of
// its set (armRetryLocked), unless the set is unfetchable; one that commits
// resets the retry's backoff.
func (p *replaySetPuller) run(ctx context.Context, f *replayFetch, ref wire.ReplaySetRef, scope cachebox.PhasePair) {
	defer p.wg.Done()
	err := p.fetch(ctx, ref, scope)
	f.cancel()

	p.mu.Lock()
	if err == nil {
		p.committedID = ref.ID
	}
	current := p.current == f
	var retrying bool
	var retryIn time.Duration
	if current {
		p.current = nil
		if err == nil {
			p.failures = 0
			p.setStateLocked(ref.ID, wire.ReplaySetCommitted, "")
		} else {
			p.setStateLocked(ref.ID, wire.ReplaySetFailed, err.Error())
			if retrying = !errors.As(err, new(unfetchableError)); retrying {
				p.failures++
				retryIn = p.armRetryLocked(ref, scope)
			}
		}
	}
	p.mu.Unlock()
	close(f.done)

	if !current {
		return
	}
	switch {
	case err == nil:
		p.logger.Info("replay set committed", "id", ref.ID, "count", ref.Count,
			"experiment_id", scope.ExperimentID, "phase_id", scope.PhaseID)
	case retrying:
		p.logger.Warn("replay set failed, retrying", "id", ref.ID, "error", err, "retry_in", retryIn)
	default:
		p.logger.Warn("replay set failed", "id", ref.ID, "error", err)
	}
	if p.onChange != nil {
		p.onChange()
	}
}

// armRetryLocked arms the retry of the current set's failed pull, ref under
// scope: once retryDelay(p.failures) has passed, its goroutine pulls the set
// again (retryFetch) unless the retry is dropped first -- by a Reconcile
// naming another set or none, or by Close. It returns the wait. p.mu must be
// held.
func (p *replaySetPuller) armRetryLocked(ref wire.ReplaySetRef, scope cachebox.PhasePair) time.Duration {
	ctx, cancel := context.WithCancel(p.ctx)
	r := &replayRetry{id: ref.ID, scope: scope, cancel: cancel}
	p.retry = r
	wait := p.retryDelay(p.failures)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer cancel()
		if sleepCtx(ctx, wait) {
			p.retryFetch(r, ref, scope)
		}
	}()
	return wait
}

// retryFetch pulls the set of the failed pull r stands for again, as a
// Reconcile naming it would: the state turns fetching -- reported through
// OnChange, so manteion sees failed → fetching → committed (or failed again)
// -- and a fetch starts, with a fresh attempt budget for each chunk. Nothing
// happens when r was dropped meanwhile.
func (p *replaySetPuller) retryFetch(r *replayRetry, ref wire.ReplaySetRef, scope cachebox.PhasePair) {
	p.reconcileMu.Lock()
	p.mu.Lock()
	if p.closed || p.retry != r {
		p.mu.Unlock()
		p.reconcileMu.Unlock()
		return
	}
	p.retry = nil
	p.startLocked(ref, scope)
	failures := p.failures
	p.mu.Unlock()
	p.reconcileMu.Unlock()
	p.logger.Info("replay set fetch retried", "id", ref.ID, "failed_pulls", failures,
		"experiment_id", scope.ExperimentID, "phase_id", scope.PhaseID)
	if p.onChange != nil {
		p.onChange()
	}
}

// retryDelay is the wait before the retry that follows the n-th consecutive
// failed pull of a set (n ≥ 1): retryBase·2^(n-1), at most retryMax, spread by
// jitter.
func (p *replaySetPuller) retryDelay(n int) time.Duration {
	d := p.retryBase
	for i := 1; i < n && d < p.retryMax; i++ {
		d *= 2
	}
	return p.jitter(min(d, p.retryMax))
}

// jitterRetry spreads d uniformly over ±20 %, so instances whose pulls failed
// together do not retry in step.
func jitterRetry(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.8 + 0.4*rand.Float64()))
}

// unfetchableError is a failed pull that the ref and its scope decide on
// their own, before any request: no pair scopes the set, its key strategy is
// unsupported, or its artifact URL is missing or does not parse. Pulling the
// same ref under the same scope again cannot succeed, so no retry is armed for
// it; a document naming another ref or scope starts over (Reconcile).
type unfetchableError struct{ err error }

func (e unfetchableError) Error() string { return e.err.Error() }
func (e unfetchableError) Unwrap() error { return e.err }

// fetch pulls the artifact ref names in Range chunks of replaySetChunkBytes,
// stages its entries under scope through the cache box's preload path and
// commits them; the commit installs the set only when the entry count and the
// W5 digest match ref. A chunk whose request cannot be built or sent, or is
// answered 5xx, 408 or 429, is retried from the same offset. A server that
// ignores Range and answers 200 with the whole artifact is accepted. Lines
// straddling a chunk boundary are carried into the next chunk, never split.
// Any failure aborts the staging and leaves the installed set as it was; a
// failure the ref and scope decide before any request is an unfetchableError.
func (p *replaySetPuller) fetch(ctx context.Context, ref wire.ReplaySetRef, scope cachebox.PhasePair) error {
	if scope.ExperimentID == "" || scope.PhaseID == "" {
		return unfetchableError{fmt.Errorf("replay set %s: no (experiment_id, phase_id) scopes it (no replay rule context, no freeze_cfg.context)", ref.ID)}
	}
	if r := p.cb.PreloadBegin(scope.ExperimentID, scope.PhaseID, ref.KeyStrategy, 0); r.UnsupportedStrategy {
		return unfetchableError{fmt.Errorf("replay set %s: unsupported key_strategy %q", ref.ID, ref.KeyStrategy)}
	}
	fail := func(err error) error {
		p.cb.PreloadAbort(scope.ExperimentID, scope.PhaseID)
		return err
	}
	target, err := p.resolve(ref.URL)
	if err != nil {
		return fail(unfetchableError{fmt.Errorf("replay set %s: %w", ref.ID, err)})
	}
	chunk := replaySetChunkBytes
	var offset int64
	var seq, attempt int
	var carry []byte
	for done := false; !done; {
		status, header, body, err := p.getChunk(ctx, target, offset, chunk)
		if ctx.Err() != nil {
			return fail(ctx.Err())
		}
		if err == nil && retryableStatus(status) {
			err = fmt.Errorf("status %d", status)
		}
		if err != nil {
			attempt++
			if attempt >= replaySetMaxAttempts {
				return fail(fmt.Errorf("replay set %s: fetch at offset %d: %w (gave up after %d attempts)", ref.ID, offset, err, attempt))
			}
			p.logger.Warn("replay set chunk fetch failed, retrying", "id", ref.ID, "offset", offset, "attempt", attempt, "error", err)
			if !sleepCtx(ctx, pushBackoff(attempt)) {
				return fail(ctx.Err())
			}
			continue // resume from the same offset
		}
		attempt = 0
		switch {
		case status == http.StatusPartialContent:
			total, ok := contentRangeTotal(header.Get("Content-Range")) // "bytes a-b/total"
			offset += int64(len(body))
			done = (ok && offset >= total) || int64(len(body)) < chunk
		case status == http.StatusOK && offset == 0: // the server ignored Range: the whole artifact
			offset = int64(len(body))
			done = true
		case status == http.StatusRequestedRangeNotSatisfiable && offset > 0: // the size is an exact multiple of the chunk
			body = nil // the 416's body is an error message, not artifact bytes
			done = true
		default:
			return fail(fmt.Errorf("replay set %s: unexpected status %d at offset %d", ref.ID, status, offset))
		}
		if ref.SizeBytes > 0 && offset > ref.SizeBytes {
			return fail(fmt.Errorf("replay set %s: size mismatch: fetched %d bytes, want %d", ref.ID, offset, ref.SizeBytes))
		}
		var entries []*cachebox.Entry
		entries, carry, err = decodeNDJSON(append(carry, body...), done)
		if err != nil {
			return fail(fmt.Errorf("replay set %s: %w", ref.ID, err))
		}
		if len(entries) > 0 {
			seq++
			if r := p.cb.PreloadChunk(scope.ExperimentID, scope.PhaseID, seq, entries); r.NoBegin {
				return fail(fmt.Errorf("replay set %s: staging was reset mid-fetch", ref.ID))
			}
		}
	}
	if ref.SizeBytes > 0 && offset != ref.SizeBytes {
		return fail(fmt.Errorf("replay set %s: size mismatch: fetched %d bytes, want %d", ref.ID, offset, ref.SizeBytes))
	}
	if err := ctx.Err(); err != nil { // superseded or closed: install nothing
		return fail(err)
	}
	res := p.cb.PreloadCommit(scope.ExperimentID, scope.PhaseID, ref.Count, ref.SHA256)
	if res.NoBegin {
		return fmt.Errorf("replay set %s: staging was reset before commit", ref.ID)
	}
	if !res.OK {
		return fmt.Errorf("replay set %s: commit mismatch: staged %d entries checksum %s, want %d %s", ref.ID, res.Loaded, res.Checksum, ref.Count, ref.SHA256)
	}
	return nil
}

// getChunk requests the bytes of target from offset, at most n of them, and
// reads the whole answer, within replaySetChunkTimeout. An error means the
// chunk is to be retried: the request could not be built (newRequest, which
// applies manteion auth) or sent, or its body could not be read.
func (p *replaySetPuller) getChunk(ctx context.Context, target string, offset, n int64) (int, http.Header, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, replaySetChunkTimeout)
	defer cancel()
	req, err := p.newRequest(ctx, http.MethodGet, target)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+n-1))
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body) // a 200 is the whole artifact; a 206 is at most one chunk
	if err != nil {
		return 0, nil, nil, err
	}
	return resp.StatusCode, resp.Header, body, nil
}

// retryableStatus reports whether an artifact answer is retried from the same
// offset: a 5xx, 408 Request Timeout or 429 Too Many Requests.
func retryableStatus(code int) bool {
	return code >= 500 || code == http.StatusRequestTimeout || code == http.StatusTooManyRequests
}

// resolve returns the URL to GET for an artifact url. An absolute url passes
// through. A relative one is relative to the manteion base URL the way every
// manteion endpoint is (base + "/api/v1/..."), so it is appended to the base
// rather than RFC 3986-resolved against it: a base with a path prefix keeps it.
func (p *replaySetPuller) resolve(ref string) (string, error) {
	if ref == "" {
		return "", errors.New("no artifact url")
	}
	u, err := url.Parse(ref)
	if err != nil {
		return "", fmt.Errorf("artifact url %q: %w", ref, err)
	}
	if u.IsAbs() {
		return ref, nil
	}
	return strings.TrimSuffix(p.baseURL, "/") + "/" + strings.TrimPrefix(ref, "/"), nil
}

// contentRangeTotal parses the complete length from a Content-Range header
// ("bytes a-b/total"); ok is false when it is absent or "*".
func contentRangeTotal(h string) (total int64, ok bool) {
	i := strings.LastIndexByte(h, '/')
	if i < 0 {
		return 0, false
	}
	total, err := strconv.ParseInt(strings.TrimSpace(h[i+1:]), 10, 64)
	if err != nil || total < 0 {
		return 0, false
	}
	return total, true
}

// decodeNDJSON decodes the complete lines of buf, one cachebox.WireEntry
// each, skipping blank lines, and returns the bytes after the last newline as
// the carry to prepend to the next chunk: a chunk boundary may fall anywhere
// in a line. When final is set there is no next chunk, so a non-blank carry
// is the artifact's unterminated last line and is decoded too (the returned
// carry is then nil).
func decodeNDJSON(buf []byte, final bool) ([]*cachebox.Entry, []byte, error) {
	var entries []*cachebox.Entry
	for {
		i := bytes.IndexByte(buf, '\n')
		if i < 0 {
			break
		}
		e, err := decodeNDJSONLine(buf[:i])
		if err != nil {
			return nil, nil, err
		}
		if e != nil {
			entries = append(entries, e)
		}
		buf = buf[i+1:]
	}
	if !final {
		return entries, bytes.Clone(buf), nil // a copy: the carry must not pin the chunk's buffer
	}
	e, err := decodeNDJSONLine(buf)
	if err != nil {
		return nil, nil, err
	}
	if e != nil {
		entries = append(entries, e)
	}
	return entries, nil, nil
}

// decodeNDJSONLine decodes one artifact line; a blank line yields a nil entry.
func decodeNDJSONLine(line []byte) (*cachebox.Entry, error) {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil, nil
	}
	var w cachebox.WireEntry
	if err := json.Unmarshal(line, &w); err != nil {
		return nil, fmt.Errorf("decode artifact line: %w", err)
	}
	return cachebox.WireToEntry(&w), nil
}

// sleepCtx waits for d unless ctx ends first, and reports whether d elapsed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
