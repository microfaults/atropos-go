# Desired-State Protocol v1: SDK/Sidecar Side Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every instance of the instrument (the in-process `Serve` embed and the `atroposd` sidecar) converge on manteion's materialized desired state: long-poll with `If-None-Match`, apply through one path, report what was applied (generation, ETag, unsupported rules, errors, replay-set state), pull the replay-set artifact itself, honour `freeze_cfg.context`, and stamp an `epoch` on ingest batches.

**Architecture:** All changes land in the shared core (`internal/instrument`, `internal/wire`, `internal/cachebox`, `internal/metrics`), so `serve.go` and `internal/atroposd` pick them up unchanged. `apply()` becomes the single reconciler: it takes a `wire.RuleSync`, a scope mask (the `/admin/rules` push only touches rules) and returns an `applyReport` (unsupported rules + per-rule errors) instead of failing the whole sync on one bad rule. `ManteionClient` gains generation/ETag bookkeeping, a `POST /api/v1/sdk/status` reporter, a long-poll loop that re-polls immediately after each answer, and a `replaySetPuller` that fetches the artifact in 4 MiB `Range` chunks through the existing preload staging (`PreloadBegin/Chunk/Commit`, as library calls). `CacheBox` gains a scoped delay source so `freeze_cfg.context` is honoured and visible in `/admin/cachebox` stats.

**Tech Stack:** Go 1.25 module (`go 1.25.5`) built on a Go 1.27 host; stdlib `net/http`, `encoding/json`; `prometheus/client_golang` (metrics); `httptest` fakes for manteion and the artifact server. No new module dependencies.

**Spec:** `docs/plans/2026-10-30-desired-state-protocol.md` (authoritative; sections 2, 3 and 5 are implemented here). Background: `docs/plans/2026-09-27-daemon-design-review-2.md` §3 (ADR-2). Current-state facts: the research notes' section B (paths there are `develop` paths; the equivalents in this worktree are `internal/instrument/*`, `internal/wire/*`, `internal/metrics/*`).

## Global Constraints

- Go 1.25 module; `gofmt -l .` prints nothing, `go vet ./...` and `go test -count=1 ./...` are green after every task (baseline is green at `d29abbd`).
- Exclude `.claude/` and `vendor/` from every search and edit (there is no `vendor/` directory today).
- The committed OpenAPI spec (`docs/swagger.yaml`, `docs/swagger.json`) is pre-stale and is NOT regenerated; new wire types still carry the `//@name atropos.X` annotation like their neighbours in `internal/wire`.
- Wire shapes are exactly the spec's: `RuleSync.etag` (`omitempty`), `RuleSync.replay_set` (`omitempty`, `{id,key_strategy,count,sha256,size_bytes,url}`), `StatusReport` (`instance_id, service, applied_generation, etag, unsupported[{rule,category}], errors[], replay_set{id,state}`), ingest `epoch` (int64, instance start time in unix nanoseconds). `version` keeps its name (it is the generation).
- Poll: `GET /api/v1/sdk/rules?service=S&instance_id=I&version=G&wait=30`, header `If-None-Match: "<etag>"`; `200` → apply → `POST /api/v1/sdk/status` (expect `204`) → poll again immediately; `304` → adopt `X-Atropos-Generation` and `ETag` without applying; errors keep today's backoff/re-register behaviour.
- Replay-set artifact fetch: `GET <manteion>/<replay_set.url>` with `Range: bytes=a-b` in ≤ 4 MiB chunks, resume from the failed offset, verify `count` + W5 digest (`sha256` = `cachebox.SetChecksum`) at commit; states reported are exactly `fetching|committed|failed`.
- Unsupported categories and compositions are skipped per rule, counted in `atropos_rules_unsupported_total{category}` (compositions under `category="composition"`) and reported.
- Conventional commit messages, each ending with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Only the branch SDK talks to the branch manteion (spec §5); no compatibility shims for the old push-preload flow beyond keeping the `/cachebox/preload/*` and `/admin/cachebox/delay` handlers mounted.

## Relationship to sibling plans (same date, same worktree)

- `docs/plans/2026-10-30-sdk-quick-fixes-and-transient.md` — unexecuted at `d29abbd` (the code still has `rulesAdminHandler(eval, reg, arm, opts...)` and compositions still fail the whole decode). Its Task 1 (`applyRules(rules, targets) error`, `rulesAdminHandler(targets applyTargets)`, `null` → `[]`, mounted with `c.targets(...)`) and Task 2 (compositions → `UnsupportedFaultError{Category: CapComposition}`, counted under `composition`) are subsumed by Tasks 5 and 6 here. Whichever plan runs first: Tasks 5 and 6 below state what to keep when the quick-fixes change has already landed, and the quick-fixes Tasks 1-2 become no-ops after this plan (its Task 5 doc/OpenAPI work is unaffected). One semantic difference: this plan's `POST /admin/rules` applies the rules that decode and answers `422` listing the ones that did not, where quick-fixes keeps all-or-nothing `400`; the desired-state reconciler semantics win.
- `docs/plans/2026-10-30-agent-netem.md` — touches `internal/instrument/decode.go` (`decodeFault` network case) and `capabilities.go` (`CapNetem`), and its new tests call `decodeCompiledRules`. After Task 5 here that function is `decodeRuleSet` (see its Interfaces block); if netem lands later, its test call sites use `decodeRuleSet(...).Rules` and check `len(out.Errors) == 0`.

## Review Focus

1. A `200` whose body has no `etag` and whose response carries no `ETag`/`X-Atropos-Generation` headers (a manteion build that predates the hash) must still apply, adopt `version`, and send no `If-None-Match` on the next poll — never a quoted empty string. (Pinned by `TestFetchRules_200WithoutEtagAppliesWithoutConditional`, Task 7.)
2. An artifact server that ignores `Range` and answers `200` with the whole body must still commit; one that honours it but whose NDJSON lines straddle chunk boundaries must not split or drop an entry. (Pinned by `TestReplaySetPuller_AcceptsFullBodyWhenRangeIgnored` and the unaligned chunk size in `TestReplaySetPuller_FetchesInChunksAndCommits`, Task 4.)
3. `POST /api/v1/sdk/status` failing (5xx or unreachable) must not stall the loop and must be re-sent before the next poll, so manteion's `replay_set_state=committed` barrier is never starved by one lost report. (Pinned by `TestSendStatus_MarksPendingOnFailure`, Task 7, and `TestPollLoop_ResendsPendingStatus`, Task 8.)
4. A new `replay_set.id` arriving while a fetch is still in flight must cancel the old fetch and must not let the old fetch's chunks land in the new staging (same `(experiment, phase)` pair). (Pinned by `TestReplaySetPuller_NewIDSupersedesInFlight`, Task 4.)
5. `POST /admin/rules` going through `apply()` must not clear admin fault slots, the delay source or the installed replay set — only rules. (Pinned by `TestRulesAdmin_PostTouchesOnlyRules`, Task 6.)

---

## File structure

```
internal/wire/sync.go                 NEW: RuleSync (moved from rules.go) + Etag/ReplaySet; ReplaySetRef, StatusReport,
                                      UnsupportedRule, ReplaySetStatus, replay-set state constants
internal/wire/rules.go                RuleSync removed (lines 60-80)
types.go                              root aliases for the four new wire types
wire_fidelity_test.go                 StatusReport key pinning (root package)
internal/instrument/wire_compat_test.go   etag/replay_set keys; StatusReport round trip

internal/instrument/cache_push.go     ingestEnvelope.Epoch; cachePushConfig.Epoch
internal/instrument/core.go           NewCore stamps Epoch = time.Now().UnixNano()

internal/cachebox/delay.go            DistributionDelaySource.Params()
internal/cachebox/cachebox.go         delayScope; SetScopedDelaySource/ClearDelaySource/SampleDelayFor; Stats.Delay
internal/interceptor/cachebox.go      replay_with_delay samples via SampleDelayFor(cbCtx, entry)

internal/instrument/replay_set.go     NEW: replaySetPuller (Reconcile/Status/Close, chunked Range fetch → staging → commit)
internal/instrument/replay_set_test.go NEW: fake artifact server with Range; puller tests

internal/instrument/decode.go         decodeRuleSet → ruleSetDecode{Rules, Unsupported, Errors}; compositions skipped
internal/instrument/register.go       apply(sync, targets, scope) (applyReport, error); applyTargets.ReplaySet; freeze context;
                                      absent freeze_cfg/replay_set clear
internal/instrument/admin_rules.go    rulesAdminHandler(targets) → apply(..., scopeRules)
internal/instrument/admin_cachebox.go POST /admin/cachebox/delay → applyFreezeCfg (context honoured)
internal/instrument/control_surface.go rulesAdminHandler gets the core's targets

internal/metrics/metrics.go           SyncGeneration gauge; ReplaySetState gauge vec + SetReplaySetState
internal/instrument/manteion_client.go  etag/lastReport/statusPending; fetchRulesWait; adoptGeneration; applyDesired;
                                      sendStatus/buildStatus; long-poll loop; pollClient
internal/instrument/manteion.go       pollWait default 30s; pollClient; puller construction; initial fetch with wait=0
internal/instrument/manteion_test.go  fakeManteion helper; poll/status/long-poll tests

internal/atroposd/daemon_test.go      sidecar converges from a fake manteion's desired state
README.md, AGENTS.md, CHANGELOG.md    protocol description, /admin/rules body type, Unreleased entry
```

---

### Task 1: Wire types for the desired-state protocol

**Files:**
- Create: `internal/wire/sync.go`
- Modify: `internal/wire/rules.go:60-80` (delete `RuleSync` and its comment; it moves)
- Modify: `types.go:22-34`
- Test: `internal/instrument/wire_compat_test.go:53-64`, `wire_fidelity_test.go`

**Interfaces:**
- Produces (package `wire`, file `internal/wire/sync.go`):

```go
type RuleSync struct {
	Version      uint64         `json:"version"` // the generation, kept under the old name (spec §5)
	Etag         string         `json:"etag,omitempty"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
	ReplaySet    *ReplaySetRef  `json:"replay_set,omitempty"`
} //@name atropos.RuleSync

type ReplaySetRef struct {
	ID          string `json:"id"`
	KeyStrategy string `json:"key_strategy"`
	Count       int    `json:"count"`
	SHA256      string `json:"sha256"` // W5 digest over entries (cachebox.SetChecksum)
	SizeBytes   int64  `json:"size_bytes"`
	URL         string `json:"url"` // relative to the manteion base URL
} //@name atropos.ReplaySetRef

type UnsupportedRule struct {
	Rule     string `json:"rule"`
	Category string `json:"category"`
} //@name atropos.UnsupportedRule

type ReplaySetStatus struct {
	ID    string `json:"id"`
	State string `json:"state"` // ReplaySetFetching | ReplaySetCommitted | ReplaySetFailed
} //@name atropos.ReplaySetStatus

type StatusReport struct {
	InstanceID        string            `json:"instance_id"`
	Service           string            `json:"service"`
	AppliedGeneration uint64            `json:"applied_generation"`
	Etag              string            `json:"etag"`
	Unsupported       []UnsupportedRule `json:"unsupported"`
	Errors            []string          `json:"errors"`
	ReplaySet         *ReplaySetStatus  `json:"replay_set,omitempty"`
} //@name atropos.StatusReport

const (
	ReplaySetFetching  = "fetching"
	ReplaySetCommitted = "committed"
	ReplaySetFailed    = "failed"
)
```

- Root aliases in `types.go`: `ReplaySetRef`, `ReplaySetStatus`, `StatusReport`, `UnsupportedRule`.
- `Unsupported` and `Errors` are deliberately NOT `omitempty`: the spec says an empty list clears the previous report, so the sender must emit `[]` (Task 7 builds them non-nil).

- [ ] **Step 1: Write the failing tests**

In `internal/instrument/wire_compat_test.go`, extend the key list at line 60 and add a status-report test:

```go
// in TestRuleSync_WireRoundTripAndApply: set sync.Etag = "7-9f1c0e3a7b2d4f60" and
// sync.ReplaySet = &wire.ReplaySetRef{ID: "sha256:abc", KeyStrategy: "canonical_v2", Count: 2,
//     SHA256: "deadbeef", SizeBytes: 128, URL: "/api/v1/cache/artifacts/sha256:abc"}
for _, key := range []string{`"version"`, `"etag"`, `"rules"`, `"active_faults"`, `"freeze_cfg"`, `"replay_set"`,
    `"key_strategy"`, `"size_bytes"`, `"fault_type"`, `"params"`} { ... }
// after unmarshal:
if decoded.Etag != "7-9f1c0e3a7b2d4f60" || decoded.ReplaySet == nil || decoded.ReplaySet.Count != 2 {
    t.Fatalf("etag/replay_set did not round-trip: %+v", decoded)
}

func TestStatusReport_WireKeys(t *testing.T) {
    raw, err := json.Marshal(wire.StatusReport{
        InstanceID: "pod-1", Service: "cart", AppliedGeneration: 42, Etag: "42-abc",
        Unsupported: []wire.UnsupportedRule{{Rule: "cpu-quota", Category: "resource"}},
        Errors:      []string{},
        ReplaySet:   &wire.ReplaySetStatus{ID: "sha256:abc", State: wire.ReplaySetCommitted},
    })
    if err != nil {
        t.Fatal(err)
    }
    for _, key := range []string{`"instance_id":"pod-1"`, `"service":"cart"`, `"applied_generation":42`, `"etag":"42-abc"`,
        `"unsupported":[{"rule":"cpu-quota","category":"resource"}]`, `"errors":[]`, `"replay_set":{"id":"sha256:abc","state":"committed"}`} {
        if !strings.Contains(string(raw), key) {
            t.Errorf("status report missing %s: %s", key, raw)
        }
    }
    // An empty report still carries both lists explicitly: empty clears the previous report.
    raw, _ = json.Marshal(wire.StatusReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{}})
    if !strings.Contains(string(raw), `"unsupported":[]`) || !strings.Contains(string(raw), `"errors":[]`) || strings.Contains(string(raw), `"replay_set"`) {
        t.Fatalf("empty report = %s", raw)
    }
}
```

In `wire_fidelity_test.go` (root package), add a subtest to `TestWireContract_JSONRoundTrip` that marshals `StatusReport{InstanceID: "i", Service: "s", AppliedGeneration: 1, Etag: "1-a", Unsupported: []UnsupportedRule{}, Errors: []string{}}` and asserts the keys `"instance_id"`, `"applied_generation"`, `"unsupported"`, `"errors"` are present and `"replay_set"` is absent — this proves the root aliases exist.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run 'TestRuleSync_WireRoundTripAndApply|TestStatusReport_WireKeys' 2>&1 | head -20 && go test -count=1 . -run TestWireContract_JSONRoundTrip 2>&1 | head`
Expected: build failure `undefined: wire.ReplaySetRef` / `sync.Etag undefined` / `undefined: StatusReport`.

- [ ] **Step 3: Create `internal/wire/sync.go` and move `RuleSync` into it**

Delete lines 60-80 of `internal/wire/rules.go` (the `RuleSync` doc comment and struct). Create `internal/wire/sync.go` with the five declarations and the three constants from the Interfaces block. Keep the existing `RuleSync` doc comment (the "Rules and ActiveFaults are deliberately NOT omitempty" paragraph) and add one sentence each for `Etag` ("the ETag of this materialized document: `<generation>-<sha256 hex[:16]>`; sent back as `If-None-Match`") and `ReplaySet` ("absent means no replay set is desired: the instance clears one it holds"). Add the four aliases to `types.go` in the "Faults and rules" block.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./internal/instrument -run 'TestRuleSync|TestStatusReport' && go test -count=1 . -run TestWireContract_JSONRoundTrip`
Expected: no gofmt output; `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument` and `ok  	git.ucsc.edu/microfaults/atropos-go`.

- [ ] **Step 5: Commit**

```bash
git add internal/wire/sync.go internal/wire/rules.go types.go wire_fidelity_test.go internal/instrument/wire_compat_test.go
git commit -m "feat(wire): desired-state sync types: RuleSync etag/replay_set, StatusReport, ReplaySetRef

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Ingest batches carry the instance epoch

**Files:**
- Modify: `internal/instrument/cache_push.go:18-27` (config), `:79-88` (struct), `:120-144` (constructor), `:281-288` (envelope), `:297-307` (post)
- Modify: `internal/instrument/core.go:96-106`
- Test: `internal/instrument/cache_push_test.go`, `internal/instrument/core_test.go`

**Interfaces:**
- Produces: `cachePushConfig.Epoch int64` (unix nanos; 0 = unset, marshals as `"epoch":0`), `cachePushClient.epoch int64`, `ingestEnvelope.Epoch int64 \`json:"epoch"\``. `NewCore` sets `Epoch: time.Now().UnixNano()` once, before building the push client; `Core.push.epoch` is readable by tests.

- [ ] **Step 1: Write the failing tests**

In `internal/instrument/cache_push_test.go`:

```go
// TestPush_EnvelopeCarriesEpoch pins the (instance, epoch, batch_seq) dedupe key
// (spec §2, ingest): every batch from one process carries the same epoch.
func TestPush_EnvelopeCarriesEpoch(t *testing.T) {
    var mu sync.Mutex
    var epochs []int64
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var env struct {
            Epoch int64 `json:"epoch"`
        }
        b, _ := io.ReadAll(r.Body)
        _ = json.Unmarshal(b, &env)
        mu.Lock()
        epochs = append(epochs, env.Epoch)
        mu.Unlock()
        w.WriteHeader(http.StatusCreated)
    }))
    defer server.Close()

    client := newCachePushClient(cachePushConfig{
        BaseURL: server.URL, Service: "cart", Instance: "pod-1", Epoch: 1700000000000000001,
        MaxBatch: 1, MaxWait: 10 * time.Second,
    })
    defer client.Stop()
    for i := 0; i < 2; i++ {
        client.PushFunc()("k", &cachebox.Entry{Key: "k", StatusCode: 200, Body: []byte("x"), ExperimentID: "exp-1", PhaseID: "phase-1"})
        want := int64(i + 1)
        waitFor(t, 5*time.Second, func() bool { return client.Stats().Pushed == want })
    }
    mu.Lock()
    defer mu.Unlock()
    if len(epochs) != 2 || epochs[0] != 1700000000000000001 || epochs[1] != 1700000000000000001 {
        t.Fatalf("epochs = %v, want the configured epoch on every batch", epochs)
    }
}
```

In `internal/instrument/core_test.go`, inside `TestNewCore_ConnectedBuildsPushAndDrainAndAdoptsRegistry` after the `c.push == nil` check:

```go
if c.push.epoch <= 0 {
    t.Fatalf("push client epoch = %d, want the instance start time", c.push.epoch)
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run 'TestPush_EnvelopeCarriesEpoch|TestNewCore_Connected' 2>&1 | head`
Expected: build failure `unknown field Epoch in struct literal` / `c.push.epoch undefined`.

- [ ] **Step 3: Implement**

Add `Epoch int64` to `cachePushConfig` (doc: "instance start time, unix nanoseconds; dedupe key component `(instance, epoch, batch_seq)`") and `epoch int64` to `cachePushClient`, copied in `newCachePushClient`. Add `Epoch int64 \`json:"epoch"\`` to `ingestEnvelope` after `BatchSeq` and set it in `post`. In `NewCore` pass `Epoch: time.Now().UnixNano()` into `cachePushConfig`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./internal/instrument`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument`.

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/cache_push.go internal/instrument/core.go internal/instrument/cache_push_test.go internal/instrument/core_test.go
git commit -m "feat(cachebox): ingest batches carry the instance epoch

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Scoped delay source in the cache-box

**Files:**
- Modify: `internal/cachebox/delay.go:46-71`
- Modify: `internal/cachebox/cachebox.go:33-46` (struct), `:289-308` (SampleDelay/SetDelaySource), `:343-362` (Stats)
- Modify: `internal/interceptor/cachebox.go:117`
- Test: `internal/cachebox/cachebox_test.go` (next to `TestCacheBox_SetDelaySource`, line 568), `internal/interceptor/cachebox_test.go` (existing `TestHandleCacheBox_ReplayDelaySleepsAtLeastObservedLatency` must keep passing)

**Interfaces:**
- Produces (package `cachebox`):
  - `func (d *DistributionDelaySource) Params() (mu, sigma float64)`
  - `func (cb *CacheBox) SetScopedDelaySource(d DelaySource, scope PhasePair)` — installs `d` for replays whose matched rule context is `scope`; a zero scope means unscoped.
  - `func (cb *CacheBox) SetDelaySource(d DelaySource)` — unchanged signature, now `SetScopedDelaySource(d, PhasePair{})`.
  - `func (cb *CacheBox) ClearDelaySource()` — back to `ObservedDelaySource{}`, zero scope.
  - `func (cb *CacheBox) SampleDelayFor(cbCtx *CacheBoxContext, entry *Entry) time.Duration` — if a scope is set and `cbCtx` is nil or its `(ExperimentID, PhaseID)` differs, returns `ObservedDelaySource{}.Sample(entry)`; otherwise the installed source's sample. `SampleDelay(entry)` becomes `SampleDelayFor(nil, entry)`.
  - `type DelayStats struct { Installed bool; ExperimentID string; PhaseID string; Mu float64; Sigma float64 }` and `Stats.Delay DelayStats` (no json tags, like the sibling fields: `/admin/cachebox` serves `cb.Stats()` verbatim). `Installed` is true iff the source is a `*DistributionDelaySource`; `Mu/Sigma` come from `Params()`.

- [ ] **Step 1: Write the failing tests**

In `internal/cachebox/cachebox_test.go`:

```go
func TestCacheBox_ScopedDelaySource_AppliesOnlyToMatchingPair(t *testing.T) {
    cb := New(Config{})
    defer cb.Stop()
    cb.SetScopedDelaySource(NewDistributionDelaySource(math.Log(1000), 0.2, 3), PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"})
    entry := &Entry{ObservedLatency: 7 * time.Millisecond}

    same := &CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1"}
    if got := cb.SampleDelayFor(same, entry); got <= 0 || got == 7*time.Millisecond {
        t.Fatalf("matching pair must sample the fitted distribution, got %s", got)
    }
    other := &CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-2"}
    if got := cb.SampleDelayFor(other, entry); got != 7*time.Millisecond {
        t.Fatalf("other pair must fall back to the observed latency, got %s", got)
    }
    if got := cb.SampleDelayFor(nil, entry); got != 7*time.Millisecond {
        t.Fatalf("no context under a scoped source must fall back to observed, got %s", got)
    }
    s := cb.Stats().Delay
    if !s.Installed || s.ExperimentID != "exp-1" || s.PhaseID != "phase-1" || s.Mu != math.Log(1000) || s.Sigma != 0.2 {
        t.Fatalf("Stats().Delay = %+v", s)
    }
}

func TestCacheBox_ClearDelaySource_RestoresObserved(t *testing.T) {
    cb := New(Config{})
    defer cb.Stop()
    cb.SetScopedDelaySource(NewDistributionDelaySource(math.Log(1000), 0.2, 3), PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"})
    cb.ClearDelaySource()
    entry := &Entry{ObservedLatency: 7 * time.Millisecond}
    if got := cb.SampleDelayFor(&CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1"}, entry); got != 7*time.Millisecond {
        t.Fatalf("cleared source must be the observed latency, got %s", got)
    }
    if s := cb.Stats().Delay; s.Installed || s.ExperimentID != "" {
        t.Fatalf("Stats().Delay after clear = %+v", s)
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/cachebox -run 'TestCacheBox_ScopedDelaySource|TestCacheBox_ClearDelaySource' 2>&1 | head`
Expected: build failure `cb.SetScopedDelaySource undefined`.

- [ ] **Step 3: Implement**

`delay.go`: add `Params()` (read under `d.mu.RLock`). `cachebox.go`: add `delayScope PhasePair` next to `delay` (guarded by `cb.mu`); implement the four methods from the Interfaces block; extend `Stats` with `Delay DelayStats` and fill it in `Stats()` under `cb.mu.RLock`. `PhasePair.isZero()` already exists (`fidelity.go:129`); reuse it for the scope check. `internal/interceptor/cachebox.go:117`: `delay := cb.SampleDelayFor(cbCtx, entry)`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./internal/cachebox ./internal/interceptor ./internal/instrument`
Expected: three `ok` lines.

- [ ] **Step 5: Commit**

```bash
git add internal/cachebox/delay.go internal/cachebox/cachebox.go internal/cachebox/cachebox_test.go internal/interceptor/cachebox.go
git commit -m "feat(cachebox): delay source scoped to (experiment_id, phase_id), exposed in Stats

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Replay-set puller (chunked Range fetch through preload staging)

**Files:**
- Create: `internal/instrument/replay_set.go`
- Modify: `internal/metrics/metrics.go:81-103`
- Test: `internal/instrument/replay_set_test.go` (new)

**Interfaces:**
- Consumes: `cachebox.CacheBox.PreloadBegin/PreloadChunk/PreloadCommit/PreloadAbort/ClearReplaySet/ReplaySetPhaseKey` (`internal/cachebox/cachebox.go:257-287`), `cachebox.WireToEntry`, `cachebox.PhaseKey`, `wire.ReplaySetRef`, `wire.ReplaySetStatus`, `pushBackoff(attempt)` (`cache_push.go:374`).
- Produces (package `instrument`):
  - `var replaySetChunkBytes int64 = 4 << 20` (tests lower it); `const replaySetMaxAttempts = 5`; `const replaySetChunkTimeout = 30 * time.Second`.
  - `type replaySetPullerConfig struct { BaseURL string; Client *http.Client; NewRequest func(ctx context.Context, method, url string) (*http.Request, error); CacheBox *cachebox.CacheBox; Logger *slog.Logger; OnChange func() }` — `NewRequest` nil means `http.NewRequestWithContext`; `Client` nil means a client with no `Timeout` (per-chunk timeouts come from the context); `OnChange` is called after every asynchronous transition to `committed` or `failed`, from the fetch goroutine.
  - `func newReplaySetPuller(cfg replaySetPullerConfig) *replaySetPuller`
  - `func (p *replaySetPuller) Reconcile(ref *wire.ReplaySetRef, scope cachebox.PhasePair)` — non-blocking: `ref == nil` cancels any fetch, clears the installed set if `CacheBox.ReplaySetPhaseKey() != ""`, and resets state to none; `ref.ID` equal to the committed id *and* `ReplaySetPhaseKey() == cachebox.PhaseKey(scope...)` is a no-op; an id already being fetched is a no-op; otherwise it cancels and waits for any in-flight fetch, sets state `fetching`, and starts `fetch` in a goroutine.
  - `func (p *replaySetPuller) Status() (status *wire.ReplaySetStatus, errMsg string)` — nil when no set is desired; `errMsg` is the failure reason (non-empty only in state `failed`).
  - `func (p *replaySetPuller) Close()` — cancels and waits for the fetch goroutine.
  - `func (p *replaySetPuller) fetch(ctx context.Context, ref wire.ReplaySetRef, scope cachebox.PhasePair) error` — the algorithm below; nil-safe on the receiver for `Reconcile`/`Status`/`Close` (a nil puller means "no replay-set support").
  - `metrics.ReplaySetState = prometheus.NewGaugeVec(GaugeOpts{Name: "atropos_replay_set_state", Help: "Replay-set pull state of this instance (1 for the current state)."}, []string{"state"})` and `func SetReplaySetState(state string)` which sets `fetching|committed|failed` to 0 and `state` (if non-empty) to 1.

Fetch algorithm (the one part the signatures do not determine):

```go
func (p *replaySetPuller) fetch(ctx context.Context, ref wire.ReplaySetRef, scope cachebox.PhasePair) error {
    if scope.ExperimentID == "" || scope.PhaseID == "" {
        return fmt.Errorf("replay set %s: no (experiment_id, phase_id) scopes it (no replay rule context, no freeze_cfg.context)", ref.ID)
    }
    if r := p.cb.PreloadBegin(scope.ExperimentID, scope.PhaseID, ref.KeyStrategy, 0); r.UnsupportedStrategy {
        return fmt.Errorf("replay set %s: unsupported key_strategy %q", ref.ID, ref.KeyStrategy)
    }
    fail := func(err error) error { p.cb.PreloadAbort(scope.ExperimentID, scope.PhaseID); return err }
    target := p.resolve(ref.URL) // url.Parse(BaseURL).ResolveReference(url.Parse(ref.URL)); absolute URLs pass through
    var offset int64
    var seq, attempt int
    var carry []byte
    for done := false; !done; {
        cctx, cancel := context.WithTimeout(ctx, replaySetChunkTimeout)
        req, err := p.newRequest(cctx, http.MethodGet, target)
        if err != nil { cancel(); return fail(err) }
        req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+replaySetChunkBytes-1))
        resp, err := p.client.Do(req)
        var body []byte
        if err == nil {
            body, err = io.ReadAll(resp.Body) // a 200 is the whole artifact; a 206 is at most one chunk
            resp.Body.Close()
        }
        cancel()
        if ctx.Err() != nil { return fail(ctx.Err()) }
        if err != nil || resp.StatusCode >= 500 {
            attempt++
            if attempt > replaySetMaxAttempts {
                return fail(fmt.Errorf("replay set %s: fetch at offset %d: %v (status %d)", ref.ID, offset, err, statusOf(resp)))
            }
            time.Sleep(pushBackoff(attempt)) // resume from the same offset
            continue
        }
        attempt = 0
        switch {
        case resp.StatusCode == http.StatusPartialContent:
            total, ok := contentRangeTotal(resp.Header.Get("Content-Range")) // "bytes a-b/total"
            offset += int64(len(body))
            done = (ok && offset >= total) || int64(len(body)) < replaySetChunkBytes
        case resp.StatusCode == http.StatusOK && offset == 0: // server ignored Range: whole body
            offset = int64(len(body)); done = true
        case resp.StatusCode == http.StatusRequestedRangeNotSatisfiable && offset > 0: // exact multiple of the chunk size
            done = true
        default:
            return fail(fmt.Errorf("replay set %s: unexpected status %d at offset %d", ref.ID, resp.StatusCode, offset))
        }
        var entries []*cachebox.Entry
        entries, carry, err = decodeNDJSON(append(carry, body...), done) // complete lines → entries; trailing partial line → carry (decoded too when done)
        if err != nil { return fail(fmt.Errorf("replay set %s: %w", ref.ID, err)) }
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
    res := p.cb.PreloadCommit(scope.ExperimentID, scope.PhaseID, ref.Count, ref.SHA256)
    if !res.OK {
        return fmt.Errorf("replay set %s: commit mismatch: staged %d entries checksum %s, want %d %s", ref.ID, res.Loaded, res.Checksum, ref.Count, ref.SHA256)
    }
    return nil
}
```

`decodeNDJSON(buf []byte, final bool) ([]*cachebox.Entry, []byte, error)` splits on `'\n'`, skips blank lines, `json.Unmarshal`s each line into `cachebox.WireEntry` and converts with `cachebox.WireToEntry`; the bytes after the last newline are returned as the carry, except when `final` is true, in which case a non-blank carry is decoded as the last line. `Reconcile` must `cancel()` and `<-done` of a superseded fetch *before* calling `PreloadBegin` for the new one — both target the same `(experiment, phase)` staging. The goroutine, on return, takes the lock, ignores its result if it has been superseded, otherwise records `committed` (and the committed id) or `failed` + message, calls `metrics.SetReplaySetState`, releases the lock, then calls `OnChange`.

- [ ] **Step 1: Write the failing tests**

`internal/instrument/replay_set_test.go` — a fake artifact server built on `http.ServeContent` (it implements `Range`, `206` and `Content-Range` for free) with hooks, plus the tests:

```go
type fakeArtifact struct {
    srv    *httptest.Server
    body   []byte
    mu     sync.Mutex
    ranges []string        // Range headers seen, in order
    failAt map[int64]int   // offset → remaining 500s to serve
    ignoreRange bool       // answer 200 with the whole body
    gate   chan struct{}   // when non-nil, every request blocks until it is closed
}

func newFakeArtifact(t *testing.T, entries []*cachebox.Entry) *fakeArtifact {
    t.Helper()
    var buf bytes.Buffer
    for _, e := range entries {
        line, _ := json.Marshal(cachebox.EntryToWire(e))
        buf.Write(line); buf.WriteByte('\n')
    }
    f := &fakeArtifact{body: buf.Bytes(), failAt: map[int64]int{}}
    f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if f.gate != nil { <-f.gate }
        f.mu.Lock(); f.ranges = append(f.ranges, r.Header.Get("Range")); f.mu.Unlock()
        var off int64
        fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &off)
        f.mu.Lock(); n := f.failAt[off]; if n > 0 { f.failAt[off] = n - 1 }; f.mu.Unlock()
        if n > 0 { w.WriteHeader(http.StatusInternalServerError); return }
        if f.ignoreRange { w.Write(f.body); return }
        http.ServeContent(w, r, "artifact", time.Time{}, bytes.NewReader(f.body))
    }))
    t.Cleanup(f.srv.Close)
    return f
}

func replayEntries(n int) []*cachebox.Entry // n entries, Key "v2:k<i>", 200, Body 40 bytes of 'x'+i, RecordedAt fixed
func refFor(f *fakeArtifact, entries []*cachebox.Entry) wire.ReplaySetRef {
    return wire.ReplaySetRef{ID: "sha256:" + fmt.Sprintf("%x", sha256.Sum256(f.body)), KeyStrategy: "canonical_v2",
        Count: len(entries), SHA256: cachebox.SetChecksum(entries), SizeBytes: int64(len(f.body)), URL: "/api/v1/cache/artifacts/x"}
}

func newTestPuller(t *testing.T, f *fakeArtifact, cb *cachebox.CacheBox, onChange func()) *replaySetPuller {
    t.Helper()
    p := newReplaySetPuller(replaySetPullerConfig{BaseURL: f.srv.URL, CacheBox: cb, Logger: slog.New(slog.DiscardHandler), OnChange: onChange})
    t.Cleanup(p.Close)
    return p
}

func waitState(t *testing.T, p *replaySetPuller, want string) (*wire.ReplaySetStatus, string) {
    t.Helper()
    deadline := time.Now().Add(5 * time.Second)
    for time.Now().Before(deadline) {
        if st, msg := p.Status(); st != nil && st.State == want { return st, msg }
        time.Sleep(10 * time.Millisecond)
    }
    st, msg := p.Status()
    t.Fatalf("puller state = %+v (%s), want %s", st, msg, want)
    return nil, ""
}

var scope1 = cachebox.PhasePair{ExperimentID: "exp-1", PhaseID: "phase-1"}

func TestReplaySetPuller_FetchesInChunksAndCommits(t *testing.T) {
    replaySetChunkBytes = 100 // not a multiple of the ~120-byte lines: lines straddle chunks
    t.Cleanup(func() { replaySetChunkBytes = 4 << 20 })
    entries := replayEntries(5)
    f := newFakeArtifact(t, entries)
    cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
    t.Cleanup(cb.Stop)
    var changes atomic.Int32
    p := newTestPuller(t, f, cb, func() { changes.Add(1) })

    p.Reconcile(ptr(refFor(f, entries)), scope1)
    if st, _ := p.Status(); st == nil || st.State != wire.ReplaySetFetching {
        t.Fatalf("state right after Reconcile = %+v, want fetching", st)
    }
    st, msg := waitState(t, p, wire.ReplaySetCommitted)
    if msg != "" || st.ID != refFor(f, entries).ID { t.Fatalf("status = %+v msg=%q", st, msg) }
    if cb.ReplaySetLen() != 5 || cb.ReplaySetPhaseKey() != cachebox.PhaseKey("exp-1", "phase-1") {
        t.Fatalf("installed %d entries under %q", cb.ReplaySetLen(), cb.ReplaySetPhaseKey())
    }
    if _, ok := cb.Lookup("v2:k3"); !ok { t.Fatal("entry v2:k3 missing after commit") }
    f.mu.Lock(); n := len(f.ranges); first := f.ranges[0]; f.mu.Unlock()
    if n < 5 || first != "bytes=0-99" { t.Fatalf("ranges = %d first=%q, want ≥5 chunked requests starting at bytes=0-99", n, first) }
    if changes.Load() != 1 { t.Fatalf("OnChange calls = %d, want 1", changes.Load()) }
    if snap := cb.Fidelity().Snapshot(scope1); !snap.PreloadCommitted || snap.PreloadEntries != 5 {
        t.Fatalf("fidelity preload state = %+v", snap)
    }
    // Same id again: no new fetch, state stays committed.
    p.Reconcile(ptr(refFor(f, entries)), scope1)
    if st, _ := p.Status(); st.State != wire.ReplaySetCommitted { t.Fatalf("re-reconcile state = %+v", st) }
    f.mu.Lock(); again := len(f.ranges); f.mu.Unlock()
    if again != n { t.Fatalf("same id must not refetch: %d → %d requests", n, again) }
}

func TestReplaySetPuller_ResumesAfterTransportError(t *testing.T)
// replaySetChunkBytes = 100; failAt[100] = 1 → the second chunk 500s once; expect committed and
// f.ranges to contain "bytes=100-199" twice (resume from the same offset).

func TestReplaySetPuller_AcceptsFullBodyWhenRangeIgnored(t *testing.T)
// f.ignoreRange = true; expect committed with ReplaySetLen() == len(entries).

func TestReplaySetPuller_DigestMismatchFails(t *testing.T)
// ref.SHA256 = "0000…"; expect state failed, msg contains "commit mismatch", cb.ReplaySetLen() == 0,
// a prior set installed via cb.InstallReplaySet("other", …) untouched, OnChange called once.

func TestReplaySetPuller_SizeMismatchFails(t *testing.T)
// ref.SizeBytes = len(body)+1 → failed, msg contains "size mismatch", nothing installed.

func TestReplaySetPuller_UnscopedFails(t *testing.T)
// Reconcile(ref, cachebox.PhasePair{}) → failed, msg contains "no (experiment_id, phase_id)", no HTTP request made.

func TestReplaySetPuller_NewIDSupersedesInFlight(t *testing.T)
// f1 gated (f1.gate = make(chan struct{})) so its fetch blocks; the puller's BaseURL is f1's; Reconcile(ref1);
// then Reconcile(ref2) where ref2 = refFor(f2, entries2) for a second, ungated artifact f2 (3 entries) with
// ref2.URL = f2.srv.URL + "/api/v1/cache/artifacts/y" (absolute URLs pass through resolve); close(f1.gate)
// afterwards. Expect: committed with ID == ref2.ID, ReplaySetLen() == 3, Lookup of an f1-only key misses,
// exactly one OnChange for ref2 (the superseded fetch must not report).

func TestReplaySetPuller_NilClearsInstalledSet(t *testing.T)
// commit ref1; Reconcile(nil, scope1) → Status() == (nil, ""), cb.ReplaySetLen() == 0, ReplaySetPhaseKey() == "".

func TestReplaySetPuller_RefetchesWhenInstalledSetWasClearedUnderneath(t *testing.T)
// commit ref1; cb.ClearReplaySet() (thaw via DELETE /admin/cachebox); Reconcile(ref1) → fetches again → committed, Len back to 5.
```

`ptr` is `func ptr[T any](v T) *T { return &v }` in the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run TestReplaySetPuller 2>&1 | head`
Expected: build failure `undefined: newReplaySetPuller`.

- [ ] **Step 3: Implement `internal/instrument/replay_set.go` and the metric**

Implement the Interfaces block and the fetch algorithm. Internal state under one mutex: `committedID`, `state string` (`""|fetching|committed|failed`), `id`, `errMsg`, `current *replayFetch{id string; cancel context.CancelFunc; done chan struct{}}`, plus a `sync.WaitGroup` for `Close`. Log transitions at Info (`committed`, with count) and Warn (`failed`, with the message). Add `ReplaySetState` and `SetReplaySetState` to `internal/metrics/metrics.go` and register the gauge vec in `init`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 -race ./internal/instrument -run TestReplaySetPuller -v 2>&1 | grep -E '^(--- |ok|FAIL|PASS)'`
Expected: every `TestReplaySetPuller_*` line reads `--- PASS`, then `PASS`/`ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/replay_set.go internal/instrument/replay_set_test.go internal/metrics/metrics.go
git commit -m "feat(instrument): replay-set puller: chunked Range fetch staged through preload, W5-verified commit

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Per-rule decode outcome (unsupported, composition, decode errors)

**Files:**
- Modify: `internal/instrument/decode.go:89-116` (replace `decodeCompiledRules`), `:141-148` (composition branch)
- Test: `internal/instrument/decode_test.go`, `internal/instrument/capabilities_test.go:42-61,131-153`

**Interfaces:**
- Produces (package `instrument`):
  - `type ruleSetDecode struct { Rules []evaluator.StaticRule; Unsupported []wire.UnsupportedRule; Errors []string }`
  - `func decodeRuleSet(compiled []wire.CompiledRule, opts ...decodeOption) ruleSetDecode` — sorted by priority as today; a rule whose fault category is outside the capabilities (or `quota` without a resource host) → `Unsupported{Rule: name, Category: unsup.Category}` + `metrics.RulesUnsupportedTotal{category}`; a rule with `Composition != nil` → `Unsupported{Rule: name, Category: "composition"}` + `metrics.RulesUnsupportedTotal{composition}`; any other decode error → `Errors` entry formatted `fmt.Sprintf("rule %s: decode: %v", name, err)`; `Rules` holds everything that decoded. All three slices are non-nil (empty, not null) so they marshal as `[]`.
  - `decodeCompiledRules` is deleted; `decodeCompiledRule` (singular) keeps its signature and returns `*UnsupportedFaultError{Category: CapComposition, FaultType: cr.Composition.Name}` for a composition (replacing the v6 error at lines 141-148; the check moves before the fault decode). `const CapComposition = "composition"` is added to `internal/instrument/capabilities.go:7-12` — it is a report/metric label, never an advertised capability.
- Idempotence against the quick-fixes plan: if its Task 2 already landed, `CapComposition` and the composition branch exist — keep them and only add `decodeRuleSet` with the `Errors` collection (its `TestDecode_CompositionSkippedAndCounted`-style test keeps passing once converted like the others below).

- [ ] **Step 1: Write the failing tests**

Convert `internal/instrument/decode_test.go` mechanically (20 call sites):
- success sites `rules, err := decodeCompiledRules(X)` + `if err != nil { t.Fatalf(...) }` → `out := decodeRuleSet(X)` + `if len(out.Errors) != 0 || len(out.Unsupported) != 0 { t.Fatalf("decode: errors=%v unsupported=%v", out.Errors, out.Unsupported) }` + `rules := out.Rules`;
- error sites `_, err := decodeCompiledRules(X)` + `if err == nil { t.Fatal(...) }` → `out := decodeRuleSet(X)` + `if len(out.Rules) != 0 || len(out.Errors) != 1 { t.Fatalf("rules=%d errors=%v, want the rule skipped with one error", len(out.Rules), out.Errors) }`, and any `err.Error()` → `out.Errors[0]`.

Replace `TestDecodeCompiledRules_CompositionRejected` (line 385) with:

```go
func TestDecodeRuleSet_CompositionSkippedAndCounted(t *testing.T) {
    before := unsupportedCount(t, "composition")
    compiled := []wire.CompiledRule{{
        Name: "comp-rule", Mode: "inline",
        Composition: &wire.CompiledComposition{Name: "x", ExecutionMode: "parallel", Members: []wire.CompiledCompositionMember{
            {Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
        }},
    }, {
        Name: "lat", InjectionPoint: "ingress", Mode: "inline",
        Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)},
    }}
    out := decodeRuleSet(compiled)
    if len(out.Rules) != 1 || out.Rules[0].Name != "lat" || len(out.Errors) != 0 {
        t.Fatalf("out = %+v, want only lat applied and no errors", out)
    }
    if len(out.Unsupported) != 1 || out.Unsupported[0] != (wire.UnsupportedRule{Rule: "comp-rule", Category: CapComposition}) {
        t.Fatalf("unsupported = %+v", out.Unsupported)
    }
    if got := unsupportedCount(t, "composition") - before; got != 1 {
        t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
    }
}

func TestDecodeRuleSet_DecodeErrorSkipsOnlyThatRule(t *testing.T) {
    compiled := []wire.CompiledRule{
        {Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}},
        {Name: "good", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}},
    }
    out := decodeRuleSet(compiled)
    if len(out.Rules) != 1 || out.Rules[0].Name != "good" {
        t.Fatalf("rules = %+v, want only good", out.Rules)
    }
    if len(out.Errors) != 1 || !strings.HasPrefix(out.Errors[0], "rule bad: decode: ") || !strings.Contains(out.Errors[0], "bogus") {
        t.Fatalf("errors = %v", out.Errors)
    }
    if out.Unsupported == nil || out.Errors == nil {
        t.Fatal("slices must be non-nil so the status report marshals [] not null")
    }
}
```

In `capabilities_test.go`, `TestDecode_UnsupportedCategoryIsSkippedAndCounted` and `TestDecode_NoCapabilitiesMeansEverythingSupported` and `TestDecode_QuotaMechanismNeedsAResourceHost` switch to `decodeRuleSet` the same way, and the first additionally asserts `out.Unsupported[0] == wire.UnsupportedRule{Rule: "cpu", Category: "resource"}`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run 'TestDecode' 2>&1 | head`
Expected: build failure `undefined: decodeRuleSet`.

- [ ] **Step 3: Implement**

Replace `decodeCompiledRules` with `decodeRuleSet` per the Interfaces block (keep the priority sort and the `slog` warnings; the warning for a decode error is `"rule skipped: decode error"` with `rule` and `error` fields). `register.go:126` and `admin_rules.go:88` still call the old function — change both call sites minimally so the package compiles (`out := decodeRuleSet(...)`, use `out.Rules`, and for now return/400 on `len(out.Errors) > 0` exactly as before); Task 6 replaces both properly.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./internal/instrument`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument`.

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/decode.go internal/instrument/capabilities.go internal/instrument/decode_test.go internal/instrument/capabilities_test.go internal/instrument/register.go internal/instrument/admin_rules.go
git commit -m "feat(instrument): decodeRuleSet skips unsupported, composition and undecodable rules per rule and reports them

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Single apply path with report, scope, freeze context and replay-set reconcile

**Files:**
- Modify: `internal/instrument/register.go:64-93` (applyTargets), `:104-193` (apply), `:195-247` (applyActiveFault, applyFreezeCfg)
- Modify: `internal/instrument/admin_rules.go:55-100`, `internal/instrument/control_surface.go:48-53`, `internal/instrument/admin_cachebox.go:59-75`
- Test: `internal/instrument/register_test.go`, `admin_rules_test.go`, `admin_cachebox_test.go`, `capabilities_test.go:63-85`, `wire_compat_test.go:76-78,103,126`, `cache_drain_test.go:100,112,126`

**Interfaces:**
- Consumes: `decodeRuleSet` (Task 5), `replaySetPuller.Reconcile` (Task 4), `CacheBox.SetScopedDelaySource/ClearDelaySource` (Task 3).
- Produces (package `instrument`):
  - `type applyScope uint8; const ( scopeRules applyScope = 1 << iota; scopeFaults; scopeFreeze; scopeReplaySet; scopeAll = scopeRules | scopeFaults | scopeFreeze | scopeReplaySet )`
  - `type applyReport struct { Unsupported []wire.UnsupportedRule; Errors []string }` (both non-nil)
  - `func apply(sync wire.RuleSync, targets applyTargets, scope applyScope) (applyReport, error)` — the only place rules, active faults, freeze cfg and the replay set are applied. Per category, when its scope bit is set:
    - rules: `sync.Rules == nil` → no change (unchanged contract); otherwise `decodeRuleSet` → `applyRuleSet` → `targets.CacheDrain.Observe(sync.Rules)`; the decode's `Unsupported`/`Errors` go into the report;
    - active faults: reconciliation as today, but a slot whose fault is unsupported becomes `Unsupported{Rule: id, Category}` (metric as today) and a slot whose build fails becomes `Errors` entry `"active_fault <id>: <err>"`; apply continues with the remaining slots instead of returning;
    - freeze cfg: `sync.FreezeCfg != nil` → `applyFreezeCfg` (an error becomes `Errors` entry `"freeze_cfg: <err>"`); `sync.FreezeCfg == nil` and `targets.CacheBox != nil` → `targets.CacheBox.ClearDelaySource()`;
    - replay set: `targets.ReplaySet.Reconcile(sync.ReplaySet, replayScope(sync))`; `sync.ReplaySet != nil` with a nil puller → `Errors` entry `"replay_set <id>: this host has no replay-set puller"`.
    - Hard errors (returned, nothing further applied) stay exactly today's three: rules present but `Evaluator == nil`; active faults present but `DemoEval == nil`; freeze cfg present but `CacheBox == nil`.
  - `func replayScope(sync wire.RuleSync) cachebox.PhasePair` — the `(ExperimentID, PhaseID)` of the first rule with `CacheBox != nil`, `Mode` in `{"replay", "replay_with_delay"}` and a non-nil `Context` with both ids set; else `FreezeCfg.Context`'s pair when present; else zero. (The spec's `ReplaySetRef` carries no pair, but the interceptor only serves a replay set installed under the matched rule's pair — `internal/interceptor/cachebox.go:160-165`.)
  - `applyTargets.ReplaySet *replaySetPuller` (nil = no replay-set support).
  - `func applyFreezeCfg(req wire.DelayRequest, cb *cachebox.CacheBox) error` — validation as today; with `req.Context != nil` installs via `SetScopedDelaySource(src, PhasePair{req.Context.ExperimentID, req.Context.PhaseID})`, else `SetDelaySource(src)`.
  - `func rulesAdminHandler(targets applyTargets) http.Handler` — `GET` unchanged (`targets.Evaluator.Rules()`); `POST` decodes `[]wire.CompiledRule`, maps a JSON `null` to `[]wire.CompiledRule{}` (an explicit clear, as today), calls `apply(wire.RuleSync{Rules: compiled}, targets, scopeRules)`; malformed JSON → `400`; hard error → `500`; `len(report.Errors) > 0` → `422` with `{"error": "<errors joined by \"; \">"}` (the decodable rules were applied); otherwise `204`. `ControlSurface` passes `c.targets(cfg.NetworkResolver)`.
  - `handleCacheBoxDelay` (`admin_cachebox.go:59-75`) validates via `applyFreezeCfg(req, cb)` (400 on error) so the push freeze honours `context` too.
- Idempotence against the quick-fixes plan: if its Task 1 already landed, `rulesAdminHandler(targets applyTargets)` and the `c.targets(...)` mount exist and `apply`'s rules branch lives in `applyRules(rules []wire.CompiledRule, targets applyTargets) error`. Keep the handler's shape and tests; fold `applyRules` into the `scopeRules` branch of the new `apply` (delete `applyRules`, or keep it as the private rules branch returning the decode outcome — either way `apply` is the only entry point the handler and the client call), and change the handler's decode-error response from `400` to the `422`-after-partial-apply contract above.

- [ ] **Step 1: Write the failing tests**

Mechanical signature update in `register_test.go`, `capabilities_test.go`, `wire_compat_test.go`, `cache_drain_test.go`: `apply(wire.RegisterResponse{RuleSync: S}, T)` → `apply(S, T, scopeAll)`; `apply(resp, T)` where `resp` is a `RegisterResponse` → `apply(resp.RuleSync, T, scopeAll)`; `if err := apply(...); err != nil` → `if _, err := apply(...); err != nil`. Then change the four error-path tests to the report contract:

```go
func TestApply_ActiveFault_InvalidDelay(t *testing.T) {
    demo := &demoEvaluator{}
    sync := wire.RuleSync{ActiveFaults: []wire.FaultRequest{{ID: "slot-a", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}}}
    report, err := apply(sync, applyTargets{DemoEval: demo}, scopeAll)
    if err != nil {
        t.Fatalf("a bad slot must not fail the sync: %v", err)
    }
    if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "active_fault slot-a: ") || !strings.Contains(report.Errors[0], "bogus") {
        t.Fatalf("errors = %v", report.Errors)
    }
    if len(demo.ActiveIDs()) != 0 {
        t.Fatalf("bad slot must not be armed: %v", demo.ActiveIDs())
    }
}
// TestApply_ActiveFault_UnknownType: same shape, Errors[0] contains "quantum".
// TestApply_ActiveFault_NetworkRequiresResolver: err == nil, len(report.Errors) == 1.
// TestApply_FreezeCfg_NegativeMu: err == nil, Errors[0] has prefix "freeze_cfg: " and contains "mu".
```

New tests in `register_test.go`:

```go
func TestApply_ReportsUnsupportedRulesAndDecodeErrors(t *testing.T) {
    targets := applyTargets{Evaluator: newTestEvaluator(), DecodeOpts: []decodeOption{WithCapabilities(CapInline, CapCacheBox)}}
    rules := append(threeRules(), wire.CompiledRule{Name: "comp", Mode: "inline", Composition: &wire.CompiledComposition{Name: "c", ExecutionMode: "parallel"}},
        wire.CompiledRule{Name: "bad", Mode: "inline", Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"bogus"}`)}})
    report, err := apply(wire.RuleSync{Rules: rules}, targets, scopeAll)
    if err != nil {
        t.Fatal(err)
    }
    if got := len(targets.Evaluator.Rules()); got != 2 {
        t.Fatalf("applied %d rules, want 2 (lat, cb)", got)
    }
    want := []wire.UnsupportedRule{{Rule: "cpu", Category: "resource"}, {Rule: "comp", Category: "composition"}}
    if !reflect.DeepEqual(report.Unsupported, want) {
        t.Fatalf("unsupported = %+v, want %+v", report.Unsupported, want)
    }
    if len(report.Errors) != 1 || !strings.HasPrefix(report.Errors[0], "rule bad: decode: ") {
        t.Fatalf("errors = %v", report.Errors)
    }
}

func TestApply_FreezeCfgContextScopesDelay_AbsentClears(t *testing.T) {
    cb := cachebox.New(cachebox.Config{})
    defer cb.Stop()
    sync := wire.RuleSync{FreezeCfg: &wire.DelayRequest{Mu: 8.5, Sigma: 0.3, Seed: 42,
        Context: &wire.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2}}}
    if _, err := apply(sync, applyTargets{CacheBox: cb}, scopeAll); err != nil {
        t.Fatal(err)
    }
    if d := cb.Stats().Delay; !d.Installed || d.ExperimentID != "exp-1" || d.PhaseID != "phase-1" {
        t.Fatalf("Stats().Delay = %+v", d)
    }
    entry := &cachebox.Entry{ObservedLatency: 5 * time.Millisecond}
    if got := cb.SampleDelayFor(&wire.CacheBoxContext{ExperimentID: "exp-9", PhaseID: "phase-9"}, entry); got != 5*time.Millisecond {
        t.Fatalf("other phase must get the observed latency, got %v", got)
    }
    // A 200 without freeze_cfg means none desired.
    if _, err := apply(wire.RuleSync{Rules: []wire.CompiledRule{}}, applyTargets{CacheBox: cb}, scopeAll); err != nil {
        t.Fatal(err)
    }
    if d := cb.Stats().Delay; d.Installed {
        t.Fatalf("absent freeze_cfg must clear the delay source, got %+v", d)
    }
}

func TestApply_ReplaySet_ScopedFromReplayRuleContext(t *testing.T) {
    replaySetChunkBytes = 100
    t.Cleanup(func() { replaySetChunkBytes = 4 << 20 })
    entries := replayEntries(3)
    f := newFakeArtifact(t, entries)
    cb := cachebox.New(cachebox.Config{KeyStrategy: cachebox.KeyStrategyExact})
    t.Cleanup(cb.Stop)
    p := newTestPuller(t, f, cb, nil)
    targets := applyTargets{Evaluator: newTestEvaluator(), CacheBox: cb, ReplaySet: p}
    ref := refFor(f, entries)
    sync := wire.RuleSync{
        Rules: []wire.CompiledRule{{Name: "cachebox:replay:phase-1", InjectionPoint: "egress", Mode: "inline",
            CacheBox: &wire.CompiledCacheBox{Mode: "replay", Context: &wire.CacheBoxContext{ExperimentID: "exp-1", PhaseID: "phase-1", KeyStrategy: "canonical_v2", StrategyVersion: 2}}}},
        ReplaySet: &ref,
    }
    if _, err := apply(sync, targets, scopeAll); err != nil {
        t.Fatal(err)
    }
    waitState(t, p, wire.ReplaySetCommitted)
    if cb.ReplaySetPhaseKey() != cachebox.PhaseKey("exp-1", "phase-1") {
        t.Fatalf("replay set installed under %q, want the replay rule's pair", cb.ReplaySetPhaseKey())
    }
    // Absent replay_set clears it.
    if _, err := apply(wire.RuleSync{Rules: []wire.CompiledRule{}}, targets, scopeAll); err != nil {
        t.Fatal(err)
    }
    if st, _ := p.Status(); st != nil || cb.ReplaySetLen() != 0 {
        t.Fatalf("absent replay_set must clear: status=%+v len=%d", st, cb.ReplaySetLen())
    }
}

func TestApply_ReplaySetWithoutPullerIsReported(t *testing.T) // targets without ReplaySet, sync.ReplaySet set → err == nil, Errors[0] contains "no replay-set puller"

func TestReplayScope_PrefersReplayRuleThenFreezeContext(t *testing.T) // table: replay rule ctx → its pair; passthrough-only rules + freeze ctx → freeze pair; neither → zero
```

In `admin_rules_test.go`, construct the handler as `rulesAdminHandler(applyTargets{Evaluator: eval})` and add:

```go
func TestRulesAdmin_PostTouchesOnlyRules(t *testing.T) {
    cb := cachebox.New(cachebox.Config{})
    t.Cleanup(cb.Stop)
    cb.InstallReplaySet(cachebox.PhaseKey("exp-1", "phase-1"), map[string]*cachebox.Entry{"k": {Key: "k", StatusCode: 200}})
    cb.SetDelaySource(cachebox.NewDistributionDelaySource(1, 0.5, 1))
    demo := &demoEvaluator{}
    demo.Set(&evaluator.Decision{Name: "admin", Reason: "admin"}, &wire.FaultRequest{ID: "admin"})
    handler := rulesAdminHandler(applyTargets{Evaluator: evaluator.NewStaticEvaluator(), DemoEval: demo, CacheBox: cb})

    body, _ := json.Marshal([]wire.CompiledRule{{Name: "r", InjectionPoint: "egress", Mode: "inline", CacheBox: &wire.CompiledCacheBox{Mode: "replay", KeyStrategy: "exact"}}})
    rec := httptest.NewRecorder()
    handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
    if rec.Code != http.StatusNoContent {
        t.Fatalf("code = %d: %s", rec.Code, rec.Body.String())
    }
    if len(demo.ActiveIDs()) != 1 || cb.ReplaySetLen() != 1 || !cb.Stats().Delay.Installed {
        t.Fatalf("rules push must not touch slots/replay set/delay: slots=%v replay=%d delay=%+v", demo.ActiveIDs(), cb.ReplaySetLen(), cb.Stats().Delay)
    }
}

func TestRulesAdmin_PostNullClearsRules(t *testing.T) // eval seeded with one rule; POST `null` → 204 and eval.Rules() empty

func TestRulesAdmin_PostPartialDecodeIs422ButAppliesTheRest(t *testing.T) // [good, bad] → 422, body contains "rule bad: decode:", eval has 1 rule

func TestRulesAdmin_PostObservesDrain(t *testing.T) // like TestApply_EmptyRulesFiresDrainTracker (register_test.go:342) but through the handler:
// targets{Evaluator, CacheDrain: tracker}; POST recording rule then POST [] → exactly one drain report at the fake manteion.
```

In `admin_cachebox_test.go` add `"POST /delay with context scopes the delay source"`: body `{"mu":1.0,"sigma":0.5,"seed":42,"context":{"experiment_id":"exp-1","phase_id":"phase-1","key_strategy":"canonical_v2","strategy_version":2}}` → `204`; then `GET /admin/cachebox` decodes into `struct{ Delay struct{ Installed bool; ExperimentID, PhaseID string } }` and expects `Installed`, `"exp-1"`, `"phase-1"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument 2>&1 | head`
Expected: build failure `undefined: scopeAll` / `too many arguments in call to apply`.

- [ ] **Step 3: Implement**

Rewrite `apply` per the Interfaces block (keep the category-prefixed error strings and the doc comment's rules contract, and add a paragraph on scope and on "absent means none desired" for freeze cfg and replay set). `applyActiveFault` keeps its signature; `apply` turns its errors into report entries. Add `replayScope`. Change `rulesAdminHandler` and its `ControlSurface` call (`control_surface.go:53` → `rulesAdminHandler(c.targets(cfg.NetworkResolver))`; `opts` at line 48 stays for `faultAdminHandler`). Route `handleCacheBoxDelay` through `applyFreezeCfg`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 -race ./internal/instrument && go test -count=1 ./internal/atroposd . `
Expected: `ok` for `internal/instrument`, `internal/atroposd` (the sidecar e2e tests push rules via `/admin/rules`; their preloaded replay set must survive — scope) and the root package.

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/register.go internal/instrument/admin_rules.go internal/instrument/control_surface.go internal/instrument/admin_cachebox.go internal/instrument/register_test.go internal/instrument/admin_rules_test.go internal/instrument/admin_cachebox_test.go internal/instrument/capabilities_test.go internal/instrument/wire_compat_test.go internal/instrument/cache_drain_test.go
git commit -m "feat(instrument): single scoped apply path with status report; /admin/rules goes through it; freeze context and replay set reconciled

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Conditional poll, generation/ETag bookkeeping and the status report

**Files:**
- Modify: `internal/instrument/manteion_client.go:30-46` (struct), `:71-103` (Close), `:142-163` (register), `:225-281` (fetchRules)
- Modify: `internal/instrument/manteion.go:66-72,106-113` (client construction, puller), `:118-134` (config)
- Modify: `internal/metrics/metrics.go` (SyncGeneration gauge)
- Test: `internal/instrument/manteion_test.go`

**Interfaces:**
- Consumes: `apply(sync, targets, scopeAll)` (Task 6), `replaySetPuller` (Task 4), `wire.StatusReport` (Task 1).
- Produces (package `instrument`):
  - `ManteionClient` fields: `syncMu sync.Mutex` guarding `etag string`, `lastReport applyReport`, `statusPending bool`; `applyMu sync.Mutex` serializing `applyDesired`; `statusMu sync.Mutex` serializing `sendStatus`; `puller *replaySetPuller`. `ruleVersion atomic.Uint64` keeps its name and now holds the generation (`HealthStatus.RuleVersion` is unchanged).
  - `func (c *ManteionClient) fetchRulesWait(ctx context.Context, wait time.Duration) error` — query `service`, `instance_id`, `version=<ruleVersion>`, and `wait=<seconds>` only when `wait > 0`; header `If-None-Match: "<etag>"` only when the stored etag is non-empty; `304` → `adoptGeneration(resp.Header)`; `200` → decode `wire.RuleSync` (1 MiB cap as today), `etag := payload.Etag`, falling back to the `ETag` header (quotes trimmed), then `applyDesired(ctx, payload, etag, "poll")`; `503`/other → errors as today. `fetchRules(ctx)` = `fetchRulesWait(ctx, c.cfg.pollWait)`.
  - `func (c *ManteionClient) adoptGeneration(h http.Header)` — `X-Atropos-Generation` (`strconv.ParseUint`) → `ruleVersion` + `metrics.SyncGeneration.Set`; `ETag` (`strings.Trim(v, "\"")`) → `etag`; an absent or unparsable header leaves the current value.
  - `func (c *ManteionClient) applyDesired(ctx context.Context, sync wire.RuleSync, etag, source string) error` — under `applyMu`: `report, err := apply(sync, c.targets, scopeAll)`; on a hard error log `"apply failed, keeping stale state"`, set `lastReport = applyReport{Unsupported: []wire.UnsupportedRule{}, Errors: []string{err.Error()}}`, `sendStatus`, return nil; otherwise `ruleVersion = sync.Version`, `etag` stored, gauge set, `lastReport = report` (warn-log each `Errors` entry), log `"rules updated"` as today plus `generation`, `etag`, `unsupported`, then `sendStatus(ctx)`.
  - `func (c *ManteionClient) buildStatus() wire.StatusReport` — `InstanceID`, `Service`, `AppliedGeneration = ruleVersion`, `Etag`, `Unsupported`/`Errors` as copies of `lastReport` (never nil), `Errors` extended with the puller's failure message when non-empty, `ReplaySet = puller.Status()`.
  - `func (c *ManteionClient) sendStatus(ctx context.Context)` — under `statusMu`: `POST <url>/api/v1/sdk/status` through `newReq` with `Content-Type: application/json` and a 5 s timeout on `c.httpClient`; any 2xx clears `statusPending`; a transport error or non-2xx logs a Warn and sets `statusPending = true`.
  - `func (c *ManteionClient) register(ctx)` → `registerWith` then `applyDesired(ctx, resp.RuleSync, resp.Etag, "register")`.
  - `connectManteion`: when `cfg.targets.CacheBox != nil`, builds `c.puller = newReplaySetPuller(replaySetPullerConfig{BaseURL: cfg.url, Client: &http.Client{Transport: cfg.httpClient.Transport}, NewRequest: c.newReq, CacheBox: cfg.targets.CacheBox, Logger: cfg.logger, OnChange: func() { c.sendStatus(context.Background()) }})` and sets `c.targets.ReplaySet = c.puller` before `register`. `Close` calls `c.puller.Close()` after `c.cancel()` and before `c.wg.Wait()`.
  - `manteionConfig.pollWait time.Duration` (added now; `defaultManteionConfig` sets `30 * time.Second`; literal configs in tests default to 0 = today's non-held poll).
  - `metrics.SyncGeneration = prometheus.NewGauge(GaugeOpts{Name: "atropos_sync_generation", Help: "Generation of the desired state this instance holds."})`, registered in `init`.

- [ ] **Step 1: Write the failing tests**

Add a programmable fake to `internal/instrument/manteion_test.go`:

```go
type pollSeen struct{ Version, IfNoneMatch, Wait string }

type fakeManteion struct {
    srv        *httptest.Server
    mu         sync.Mutex
    polls      []pollSeen
    statuses   []wire.StatusReport
    rawStatus  [][]byte
    statusCode int // response to POST /api/v1/sdk/status; 0 = 204
    register   string // 201 body; default `{"status":"registered","version":0,"rules":[],"active_faults":[]}`
    rules      func(w http.ResponseWriter, r *http.Request) // answers GET /api/v1/sdk/rules; read and written under mu (setRules)
}

func (f *fakeManteion) setRules(fn func(http.ResponseWriter, *http.Request)) { f.mu.Lock(); f.rules = fn; f.mu.Unlock() }

func newFakeManteion(t *testing.T) *fakeManteion {
    t.Helper()
    f := &fakeManteion{register: `{"status":"registered","version":0,"rules":[],"active_faults":[]}`}
    f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        switch {
        case r.URL.Path == "/api/v1/sdk/init":
            w.WriteHeader(http.StatusOK)
        case r.URL.Path == "/api/v1/sdk/register" && r.Method == http.MethodPost:
            w.WriteHeader(http.StatusCreated)
            io.WriteString(w, f.register)
        case strings.HasPrefix(r.URL.Path, "/api/v1/sdk/register/") && r.Method == http.MethodDelete:
            w.WriteHeader(http.StatusOK)
        case r.URL.Path == "/api/v1/sdk/rules":
            q := r.URL.Query()
            f.mu.Lock()
            f.polls = append(f.polls, pollSeen{Version: q.Get("version"), IfNoneMatch: r.Header.Get("If-None-Match"), Wait: q.Get("wait")})
            fn := f.rules
            f.mu.Unlock()
            fn(w, r)
        case r.URL.Path == "/api/v1/sdk/status" && r.Method == http.MethodPost:
            b, _ := io.ReadAll(r.Body)
            var rep wire.StatusReport
            _ = json.Unmarshal(b, &rep)
            f.mu.Lock()
            f.statuses = append(f.statuses, rep)
            f.rawStatus = append(f.rawStatus, b)
            code := f.statusCode
            f.mu.Unlock()
            if code == 0 { code = http.StatusNoContent }
            w.WriteHeader(code)
        default:
            w.WriteHeader(http.StatusNotFound)
        }
    }))
    t.Cleanup(f.srv.Close)
    return f
}

func (f *fakeManteion) client(targets applyTargets) *ManteionClient {
    return &ManteionClient{
        cfg:        manteionConfig{url: f.srv.URL, serviceName: "svc", instanceID: "pod-1", pollInterval: 10 * time.Second},
        httpClient: &http.Client{Timeout: 5 * time.Second},
        targets:    targets,
        logger:     slog.New(slog.DiscardHandler),
    }
}

func (f *fakeManteion) snapshot() (polls []pollSeen, statuses []wire.StatusReport) // copies under f.mu
func (f *fakeManteion) waitStatuses(t *testing.T, n int) []wire.StatusReport      // 5 s deadline

func respond304(gen, etag string) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("ETag", `"`+etag+`"`)
        w.Header().Set("X-Atropos-Generation", gen)
        w.WriteHeader(http.StatusNotModified)
    }
}

func respond200(sync wire.RuleSync) func(http.ResponseWriter, *http.Request) {
    return func(w http.ResponseWriter, _ *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        if sync.Etag != "" { w.Header().Set("ETag", `"`+sync.Etag+`"`) }
        w.Header().Set("X-Atropos-Generation", strconv.FormatUint(sync.Version, 10))
        _ = json.NewEncoder(w).Encode(sync)
    }
}

func syncGeneration(t *testing.T) float64 // reads metrics.SyncGeneration via dto.Metric like unsupportedCount

func TestFetchRules_200AppliesAndReportsStatus(t *testing.T) {
    f := newFakeManteion(t)
    f.setRules(respond200(wire.RuleSync{Version: 42, Etag: "42-abc", ActiveFaults: []wire.FaultRequest{},
        Rules: append(threeRules(), wire.CompiledRule{Name: "comp", Mode: "inline", Composition: &wire.CompiledComposition{Name: "c", ExecutionMode: "parallel"}})}))
    eval := newTestEvaluator()
    c := f.client(applyTargets{Evaluator: eval, DecodeOpts: []decodeOption{WithCapabilities(CapInline, CapCacheBox)}})

    if err := c.fetchRules(t.Context()); err != nil {
        t.Fatal(err)
    }
    if len(eval.Rules()) != 2 || c.ruleVersion.Load() != 42 || c.currentEtag() != "42-abc" || syncGeneration(t) != 42 {
        t.Fatalf("rules=%d gen=%d etag=%q gauge=%v", len(eval.Rules()), c.ruleVersion.Load(), c.currentEtag(), syncGeneration(t))
    }
    statuses := f.waitStatuses(t, 1)
    want := wire.StatusReport{InstanceID: "pod-1", Service: "svc", AppliedGeneration: 42, Etag: "42-abc",
        Unsupported: []wire.UnsupportedRule{{Rule: "cpu", Category: "resource"}, {Rule: "comp", Category: "composition"}}, Errors: []string{}}
    if !reflect.DeepEqual(statuses[0], want) {
        t.Fatalf("status = %+v, want %+v", statuses[0], want)
    }
    if !strings.Contains(string(f.rawStatus[0]), `"errors":[]`) {
        t.Fatalf("empty errors must be sent as []: %s", f.rawStatus[0])
    }
    // The next poll is conditional on what was applied.
    f.setRules(respond304("42", "42-abc"))
    if err := c.fetchRules(t.Context()); err != nil {
        t.Fatal(err)
    }
    polls, _ := f.snapshot()
    if got := polls[1]; got.Version != "42" || got.IfNoneMatch != `"42-abc"` {
        t.Fatalf("second poll = %+v, want version=42 and If-None-Match: \"42-abc\"", got)
    }
}

func TestFetchRules_304AdoptsGenerationWithoutApplying(t *testing.T) {
    f := newFakeManteion(t)
    f.setRules(respond304("43", "43-def"))
    eval := evaluator.NewStaticEvaluator(wire.StaticRule{Name: "keep", Point: evaluator.Ingress})
    c := f.client(applyTargets{Evaluator: eval})
    if err := c.fetchRules(t.Context()); err != nil {
        t.Fatal(err)
    }
    if c.ruleVersion.Load() != 43 || c.currentEtag() != "43-def" || len(eval.Rules()) != 1 {
        t.Fatalf("gen=%d etag=%q rules=%d", c.ruleVersion.Load(), c.currentEtag(), len(eval.Rules()))
    }
    if _, statuses := f.snapshot(); len(statuses) != 0 {
        t.Fatalf("304 must not send a status report, sent %d", len(statuses))
    }
}

func TestFetchRules_200WithoutEtagAppliesWithoutConditional(t *testing.T) {
    f := newFakeManteion(t)
    f.setRules(func(w http.ResponseWriter, _ *http.Request) { // no ETag/X-Atropos-Generation headers, no etag in body
        w.Header().Set("Content-Type", "application/json")
        io.WriteString(w, `{"version":5,"rules":[],"active_faults":[]}`)
    })
    c := f.client(applyTargets{Evaluator: newTestEvaluator()})
    for i := 0; i < 2; i++ {
        if err := c.fetchRules(t.Context()); err != nil {
            t.Fatal(err)
        }
    }
    polls, _ := f.snapshot()
    if c.ruleVersion.Load() != 5 || polls[1].Version != "5" || polls[1].IfNoneMatch != "" {
        t.Fatalf("gen=%d second poll=%+v, want version=5 and no If-None-Match", c.ruleVersion.Load(), polls[1])
    }
}

func TestRegister_AdoptsStateAndReportsStatus(t *testing.T) {
    f := newFakeManteion(t)
    f.register = `{"status":"registered","version":7,"etag":"7-xyz","rules":[{"name":"lat","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"latency","params":{"delay":"1ms"}}}],"active_faults":[]}`
    eval := newTestEvaluator()
    c := f.client(applyTargets{Evaluator: eval})
    if err := c.register(t.Context()); err != nil {
        t.Fatal(err)
    }
    s := f.waitStatuses(t, 1)[0]
    if len(eval.Rules()) != 1 || c.ruleVersion.Load() != 7 || s.AppliedGeneration != 7 || s.Etag != "7-xyz" {
        t.Fatalf("rules=%d gen=%d status=%+v", len(eval.Rules()), c.ruleVersion.Load(), s)
    }
}

func TestSendStatus_MarksPendingOnFailure(t *testing.T) {
    f := newFakeManteion(t)
    f.statusCode = http.StatusInternalServerError
    f.setRules(respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}}))
    c := f.client(applyTargets{Evaluator: newTestEvaluator()})
    if err := c.fetchRules(t.Context()); err != nil {
        t.Fatalf("a failed status report must not fail the poll: %v", err)
    }
    if !c.statusIsPending() {
        t.Fatal("failed status must be marked pending for resend")
    }
    f.mu.Lock(); f.statusCode = 0; f.mu.Unlock()
    c.sendStatus(t.Context())
    if c.statusIsPending() || len(f.waitStatuses(t, 2)) != 2 {
        t.Fatal("successful resend must clear pending")
    }
}

func TestApplyDesired_HardErrorIsReportedNotAdvanced(t *testing.T) {
    f := newFakeManteion(t)
    // rules present but no Evaluator target → hard error
    f.setRules(respond200(wire.RuleSync{Version: 9, Etag: "9-a", Rules: threeRules()[:1], ActiveFaults: []wire.FaultRequest{}}))
    c := f.client(applyTargets{})
    if err := c.fetchRules(t.Context()); err != nil {
        t.Fatal(err)
    }
    s := f.waitStatuses(t, 1)[0]
    if c.ruleVersion.Load() != 0 || s.AppliedGeneration != 0 || len(s.Errors) != 1 || !strings.Contains(s.Errors[0], "apply rules") {
        t.Fatalf("gen=%d status=%+v", c.ruleVersion.Load(), s)
    }
}
```

`currentEtag()` and `statusIsPending()` are two-line accessors under `syncMu` added to `manteion_client.go` for tests and health. The existing ad-hoc servers in `TestManteionClient_PollLoop_RuleUpdate` (line 139) and `TestManteionClient_PollLoop_Recovery` (line 169) answer the new status POST with an empty `200` (their handlers write nothing for unknown paths), which `sendStatus` accepts as 2xx; they need no change.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run 'TestFetchRules_|TestRegister_Adopts|TestSendStatus_|TestApplyDesired_' 2>&1 | head`
Expected: build failure `c.currentEtag undefined` / `undefined: respond304`.

- [ ] **Step 3: Implement**

Per the Interfaces block. `fetchRules` at lines 225-281 becomes the thin wrapper plus `fetchRulesWait`; the `wire.RegisterResponse{Status: "poll", ...}` wrapping at line 261 goes away. The Info log on a 200 keeps the `rules_count`/`manual_faults` fields.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 -race ./internal/instrument && go test -count=1 . ./internal/atroposd`
Expected: `ok` for all three (the root `TestServe_OneIdentityEverywhere` fake answers `304` with no headers and `404` to status: health and identity assertions still hold).

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/manteion_client.go internal/instrument/manteion.go internal/instrument/manteion_test.go internal/metrics/metrics.go
git commit -m "feat(instrument): conditional poll (If-None-Match/version), generation adoption on 304, status report after apply

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: Long-poll loop

**Files:**
- Modify: `internal/instrument/manteion_client.go:165-223` (pollLoopWithTrigger)
- Modify: `internal/instrument/manteion.go:29-114` (connectManteion: pollClient, initial fetch), `:141-176` (defaults)
- Test: `internal/instrument/manteion_test.go`

**Interfaces:**
- Produces (package `instrument`):
  - `var pollFloor = time.Second` — the minimum gap between two polls when the server answered faster than this (guards a server that does not hold the request); tests set it to 0.
  - `ManteionClient.pollClient *http.Client` — built in `connectManteion` as `&http.Client{Transport: cfg.httpClient.Transport, Timeout: cfg.pollWait + 15*time.Second}` when `cfg.pollWait > 0`; `fetchRulesWait` uses `c.pollClient` when non-nil, else `c.httpClient` (the client timeout must exceed the server hold).
  - `pollLoopWithTrigger(ctx, trigger)` semantics: the first delay is `pollInterval` when `pollWait == 0` (today's ticker behaviour: the startup fetch already ran) and `0` when `pollWait > 0` (go straight into the held poll); then, before each poll, if `statusPending` → `sendStatus(ctx)`; `fetchRules(ctx)`; on error: `consecutiveFailures++`, status degraded, next delay = `pollInterval * 2^min(n,6)` capped at 60 s, re-register once at the 6th failure (as today); on success after failures: log restored, reset `ruleVersion`/`etag` to force a full pull, re-register in a goroutine (as today); on success: `lastPollAt`, status connected, next delay = `max(0, pollFloor - elapsed)` when `pollWait > 0`, else `pollInterval`. The wait between polls is `select { ctx.Done → return; time.After(delay); trigger }` (skipped when `delay == 0`); an SSE nudge still triggers an immediate poll.
  - `connectManteion` performs the initial fetch with `fetchRulesWait(ctx, 0)` so startup is never held for `wait`.

- [ ] **Step 1: Write the failing tests**

```go
func TestPollLoop_LongPollRepollsImmediately(t *testing.T) {
    pollFloor = 0
    t.Cleanup(func() { pollFloor = time.Second })
    f := newFakeManteion(t)
    f.setRules(func(w http.ResponseWriter, r *http.Request) {
        time.Sleep(100 * time.Millisecond) // the server "holds" the request
        respond304("3", "3-c")(w, r)
    })
    c := f.client(applyTargets{Evaluator: newTestEvaluator()})
    c.cfg.pollWait = 2 * time.Second
    c.pollClient = &http.Client{Timeout: 17 * time.Second}
    ctx, cancel := context.WithCancel(t.Context())
    c.pollCtx = ctx
    trigger := make(chan struct{}, 1)
    c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
    time.Sleep(650 * time.Millisecond)
    cancel()
    c.wg.Wait()
    polls, _ := f.snapshot()
    if len(polls) < 4 {
        t.Fatalf("got %d polls in 650ms with a 100ms hold and pollInterval=10s; the loop must re-poll immediately", len(polls))
    }
    if polls[0].Wait != "2" {
        t.Fatalf("wait query = %q, want 2 (seconds)", polls[0].Wait)
    }
    if c.ruleVersion.Load() != 3 {
        t.Fatalf("generation = %d, want 3 adopted from the 304 headers", c.ruleVersion.Load())
    }
}

func TestPollLoop_SSETriggerPollsImmediately(t *testing.T) {
    f := newFakeManteion(t)
    f.setRules(respond304("1", "1-a"))
    c := f.client(applyTargets{Evaluator: newTestEvaluator()}) // pollWait 0: ticker semantics, 10s interval
    ctx, cancel := context.WithCancel(t.Context())
    c.pollCtx = ctx
    trigger := make(chan struct{}, 1)
    c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
    time.Sleep(50 * time.Millisecond)
    trigger <- struct{}{}
    deadline := time.Now().Add(2 * time.Second)
    for time.Now().Before(deadline) {
        if p, _ := f.snapshot(); len(p) >= 1 { break }
        time.Sleep(10 * time.Millisecond)
    }
    cancel()
    c.wg.Wait()
    if p, _ := f.snapshot(); len(p) < 1 {
        t.Fatal("SSE trigger must cause an immediate poll")
    }
}

func TestPollLoop_ResendsPendingStatus(t *testing.T) {
    f := newFakeManteion(t)
    f.statusCode = http.StatusInternalServerError
    var n atomic.Int32
    f.setRules(func(w http.ResponseWriter, r *http.Request) {
        if n.Add(1) == 1 {
            respond200(wire.RuleSync{Version: 1, Etag: "1-a", Rules: []wire.CompiledRule{}, ActiveFaults: []wire.FaultRequest{}})(w, r)
            return
        }
        respond304("1", "1-a")(w, r)
    })
    c := f.client(applyTargets{Evaluator: newTestEvaluator()})
    c.cfg.pollInterval = 50 * time.Millisecond
    ctx, cancel := context.WithCancel(t.Context())
    c.pollCtx = ctx
    trigger := make(chan struct{}, 1)
    trigger <- struct{}{}
    c.wg.Go(func() { c.pollLoopWithTrigger(ctx, trigger) })
    f.waitStatuses(t, 1) // first attempt, rejected
    f.mu.Lock(); f.statusCode = 0; f.mu.Unlock()
    statuses := f.waitStatuses(t, 2) // resent before a later poll
    cancel()
    c.wg.Wait()
    if statuses[1].AppliedGeneration != 1 {
        t.Fatalf("resent status = %+v", statuses[1])
    }
}
```

`TestManteionClient_PollLoop_Recovery` (line 166) keeps passing unchanged (pollWait 0 → ticker semantics).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument -run 'TestPollLoop_' 2>&1 | head`
Expected: build failure `undefined: pollFloor` / `c.pollClient undefined`.

- [ ] **Step 3: Implement**

Rewrite `pollLoopWithTrigger` as a `for` loop with an explicit `delay` per the Interfaces block (drop the ticker). Add `pollClient` and the initial `fetchRulesWait(ctx, 0)` in `connectManteion`; set `pollWait: 30 * time.Second` in `defaultManteionConfig`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `gofmt -l . && go vet ./... && go test -count=1 -race ./internal/instrument && go test -count=1 ./...`
Expected: all `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/instrument/manteion_client.go internal/instrument/manteion.go internal/instrument/manteion_test.go
git commit -m "feat(instrument): long-poll loop (wait=30, immediate re-poll, SSE nudge kept)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Sidecar converges from manteion's desired state (end to end)

**Files:**
- Test: `internal/atroposd/daemon_test.go`

**Interfaces:**
- Consumes: everything above through `instrument.ConnectManteion` (`daemon.go:149-156`) — no sidecar code changes; `Config.ManteionURL` survives `runDaemon`'s empty `MANTEION_URL` env.

- [ ] **Step 1: Write the failing test**

```go
// TestRun_ConvergesWithManteionDesiredState drives the sidecar against a fake manteion
// speaking desired-state v1: the register response is the materialized state (a 150ms
// ingress latency rule, generation 42), the poll is conditional and long-polled, and the
// instance reports what it applied.
func TestRun_ConvergesWithManteionDesiredState(t *testing.T) {
    var mu sync.Mutex
    var polls []string // "version|If-None-Match|wait"
    var statuses []wire.StatusReport
    manteion := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        switch {
        case r.URL.Path == "/api/v1/sdk/init":
            w.WriteHeader(200)
        case r.URL.Path == "/api/v1/sdk/register" && r.Method == http.MethodPost:
            w.WriteHeader(http.StatusCreated)
            io.WriteString(w, `{"status":"registered","version":42,"etag":"42-aaaa","rules":[{"name":"lat","injection_point":"ingress","mode":"inline","fault":{"category":"inline","fault_type":"latency","params":{"delay":"150ms"}}}],"active_faults":[]}`)
        case strings.HasPrefix(r.URL.Path, "/api/v1/sdk/register/") && r.Method == http.MethodDelete:
            w.WriteHeader(200)
        case r.URL.Path == "/api/v1/sdk/rules":
            q := r.URL.Query()
            mu.Lock(); polls = append(polls, q.Get("version")+"|"+r.Header.Get("If-None-Match")+"|"+q.Get("wait")); mu.Unlock()
            w.Header().Set("ETag", `"42-aaaa"`)
            w.Header().Set("X-Atropos-Generation", "42")
            w.WriteHeader(http.StatusNotModified)
        case r.URL.Path == "/api/v1/sdk/status" && r.Method == http.MethodPost:
            var rep wire.StatusReport
            _ = json.NewDecoder(r.Body).Decode(&rep)
            mu.Lock(); statuses = append(statuses, rep); mu.Unlock()
            w.WriteHeader(http.StatusNoContent)
        default:
            w.WriteHeader(404)
        }
    }))
    t.Cleanup(manteion.Close)

    app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("app")) }))
    t.Cleanup(app.Close)
    addrs := runDaemon(t, Config{Service: "frontend", ManteionURL: manteion.URL, IngressListen: "127.0.0.1:0", InboundUpstream: strings.TrimPrefix(app.URL, "http://")})

    start := time.Now()
    if code, body := getJSON(t, "http://"+addrs.Ingress+"/x"); code != 200 || body != "app" {
        t.Fatalf("code=%d body=%q", code, body)
    }
    if el := time.Since(start); el < 150*time.Millisecond {
        t.Fatalf("the register-time rule was not applied through the shared apply path: %v", el)
    }
    deadline := time.Now().Add(5 * time.Second)
    for time.Now().Before(deadline) {
        mu.Lock(); ns, np := len(statuses), len(polls); mu.Unlock()
        if ns >= 1 && np >= 2 { break }
        time.Sleep(20 * time.Millisecond)
    }
    mu.Lock()
    defer mu.Unlock()
    if len(statuses) < 1 || statuses[0].AppliedGeneration != 42 || statuses[0].Etag != "42-aaaa" || len(statuses[0].Unsupported) != 0 || len(statuses[0].Errors) != 0 {
        t.Fatalf("statuses = %+v", statuses)
    }
    if len(polls) < 2 || polls[1] != `42|"42-aaaa"|30` {
        t.Fatalf("polls = %v, want the loop's poll to carry version=42, If-None-Match \"42-aaaa\" and wait=30 (polls[0] is the startup fetch with no wait)", polls)
    }
    if code, body := getJSON(t, "http://"+addrs.Control+"/atropos/health"); code != 200 || !strings.Contains(body, `"rule_version":42`) {
        t.Fatalf("health: code=%d body=%s", code, body)
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -count=1 ./internal/atroposd -run TestRun_ConvergesWithManteionDesiredState -v 2>&1 | tail -5`
Expected: at this point the implementation exists, so the test should PASS on the first run; if it fails, the failure names the sidecar wiring gap (for example the startup fetch carrying `wait`), which is the bug to fix in `internal/instrument/manteion.go` — the sidecar itself needs no change.

- [ ] **Step 3: Run the whole suite with the race detector on the two host packages**

Run: `gofmt -l . && go vet ./... && go test -count=1 -race ./internal/atroposd ./internal/instrument . && go test -count=1 ./...`
Expected: all `ok`; the shutdown path (`Close` cancelling a held long-poll) returns within the test's 15 s cleanup budget.

- [ ] **Step 4: Commit**

```bash
git add internal/atroposd/daemon_test.go
git commit -m "test(atroposd): sidecar converges from a fake manteion's desired state (register, conditional long-poll, status)

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Documentation

**Files:**
- Modify: `README.md:43` (protocol row), `README.md:369-390` (integration section)
- Modify: `AGENTS.md:92-99` (`/admin/rules` table), `AGENTS.md:100-135` (SDK Bootstrap, Types, Limitations)
- Modify: `CHANGELOG.md:5` (new `## Unreleased` subsection)

- [ ] **Step 1: README**

Row 43: `SDK ↔ manteion wire protocol — long-poll desired state (ETag/generation), single apply path, status report, replay-set pull, SSE nudge | Shipped`. In "Manteion integration & the fidelity pipeline": replace the first paragraph with the v1 loop (`wait=30` long-poll with `If-None-Match`; `200` → apply → `POST /api/v1/sdk/status` → poll again; `304` → adopt generation; unsupported rules and compositions skipped per rule and reported; `freeze_cfg.context` scopes the delay source); replace step 3 "Preload" with the pull (`replay_set` in desired state → `GET /api/v1/cache/artifacts/{id}` in 4 MiB `Range` chunks → staged → W5-verified commit → `replay_set.state` reported; `/cachebox/preload/*` kept for manual use); step 4 now says the delay source arrives in `freeze_cfg` (the `/admin/cachebox/delay` push remains).

- [ ] **Step 2: AGENTS.md**

`/admin/rules` table: body is `[]CompiledRule` JSON (not `[]StaticRule`); responses `204`, `400` malformed JSON, `422` when some rules did not decode (the rest were applied). If the quick-fixes plan's Task 5 already rewrote this table, only the `422` row and the "single apply path" sentence need updating. SDK Bootstrap: describe register → `RuleSync` with `version`/`etag`, the long-poll loop, `StatusReport` (`POST /api/v1/sdk/status`), replay-set pull and `epoch`; Types: add `RuleSync{Version, Etag, Rules, ActiveFaults, FreezeCfg, ReplaySet}`, `ReplaySetRef`, `StatusReport`, `UnsupportedRule`, `ReplaySetStatus`; fix `RegisterResponse{Status, RuleSync}`; Limitations: compositions are skipped per rule and reported under `category="composition"` (no longer a decode failure).

- [ ] **Step 3: CHANGELOG**

Under `## Unreleased`, before "Added — Route publication", add `### Changed — Desired-state protocol v1 (ADR-2)` with one bullet each for: long-poll + `If-None-Match`/`version`; `304` adoption; `StatusReport` after every apply and on replay-set transitions; single scoped `apply()` (`/admin/rules` now observes drain; `422` on partial decode); per-rule skipping of unsupported/composition/undecodable rules; `freeze_cfg.context` honoured and shown in `/admin/cachebox` stats, absent `freeze_cfg`/`replay_set` clear; replay-set pull; ingest `epoch`; new metrics `atropos_sync_generation`, `atropos_replay_set_state{state}`.

- [ ] **Step 4: Verify and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: all `ok` (docs only).

```bash
git add README.md AGENTS.md CHANGELOG.md
git commit -m "docs: desired-state protocol v1 on the SDK: long-poll, status report, replay-set pull, admin/rules body

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage.** §2 `GET /api/v1/sdk/rules` (`version`, `If-None-Match`, `wait`, 200/304 headers) → Tasks 7, 8. §2 `RuleSync` fields → Task 1. §2 `POST /api/v1/sdk/status` body, "after every apply" and "whenever replay_set.state changes" → Tasks 7 (`applyDesired`, `OnChange → sendStatus`) and 4. §2 artifacts (`Range`, ≤ 4 MiB, staging, count + W5, failure → `failed` with reason) → Task 4. §2 SSE unchanged → Task 8 keeps the trigger. §2 ingest `epoch` → Task 2. §2 Register response = materialized `RuleSync` → Task 7 (`register` → `applyDesired`). §3 single `apply()` incl. `/admin/rules`, drain observation once → Task 6. §3 per-rule skip + metric + report (compositions under `composition`) → Tasks 5, 6. §3 `freeze_cfg.context` scoped delay in stats → Tasks 3, 6. §3 replay set: fetch when id differs, state transitions, restarted instance converges (committed id starts empty) → Tasks 4, 6. §5 compatibility (`version` keeps its name) → Task 1. Scope items 7 (metrics) → Tasks 4, 7.

**Step scan.** Every code step names the file, signature and pinned values; the only bodies given are the fetch algorithm (Task 4) and the fake helpers tests share. Task 9's "fails first" step is honest about the test possibly passing immediately, since it is an integration test of already-landed code.

**Type consistency.** `apply(sync wire.RuleSync, targets applyTargets, scope applyScope) (applyReport, error)` is used identically in Tasks 6, 7 and the test updates; `decodeRuleSet` → `ruleSetDecode{Rules, Unsupported, Errors}` in Tasks 5 and 6; `replaySetPuller.Reconcile(*wire.ReplaySetRef, cachebox.PhasePair)` / `Status() (*wire.ReplaySetStatus, string)` in Tasks 4, 6, 7; `SampleDelayFor(*CacheBoxContext, *Entry)` in Tasks 3 and 6; `fetchRulesWait(ctx, wait)` in Tasks 7, 8, 9; `wire.ReplaySetFetching/Committed/Failed` constants from Task 1 used in Tasks 4 and 6.

**Review Focus.** All five items have their pinning test in the owning task (7, 4, 7+8, 4, 6).

**Proportion.** The plan is roughly four times the spec's length; the excess is test code (the fakes for manteion and the artifact server) and the one algorithm, not transcribed production bodies.

## Notes for the implementer

- Transitional hazard: the old push-preload flow (`POST /cachebox/preload/*` from manteion) and the desired-state poll cannot be mixed — a `200` without `replay_set` clears whatever is installed. The branch manteion assigns artifacts instead of pushing (spec §4), so this is by design.
- `ReplaySetRef` carries no `(experiment_id, phase_id)`; the scope comes from the synthesized replay rule's `CacheBoxContext` (fallback `freeze_cfg.context`). If manteion ever ships a `replay_set` without either, the instance reports `failed` with a clear message rather than guessing.
- `pollFloor` only matters against a server that does not hold the request; with the branch manteion the loop is driven by the server's 30 s hold.
