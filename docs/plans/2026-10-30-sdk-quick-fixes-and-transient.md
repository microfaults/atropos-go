# SDK quick fixes and Transient injection point Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four SDK-side gaps the daemon design review left in its §5 table: one rule-apply path for push and poll, per-rule skipping of composition rules, a working Transient injection point on HTTP ingress, and an `AGENTS.md` that matches the wire.

**Architecture:** The rules branch of `apply` becomes `applyRules`, the only function that decodes, replaces, stops, arms and drain-observes a rule set; `POST /admin/rules` calls it instead of re-implementing half of it. Composition rules return the existing `UnsupportedFaultError` so the decoder's per-rule skip path handles them like an unsupported category. The Transient point is evaluated by the interceptor's ingress middleware (inside the otelhttp span) once the response is complete, with the ingress labels plus `atropos.http.status_code`; a new `Interceptor.StartTransient` runs the matching fault in background mode through the fault registry under a context detached from the request but still carrying its trace context, so the fault span is a child of the request span and outlives the request. Both hosts get it for free: the sidecar wraps its reverse proxies in the same `instrument.IngressMiddleware` (`internal/proxy/ingress.go:31`, `internal/proxy/transparent.go:154`).

**Tech Stack:** Go 1.25 module (`go 1.25.5` in `go.mod`; host `go1.27.2`), `net/http`, OpenTelemetry (`otelhttp`, `otel/trace`), Prometheus `client_golang` with `client_model` (`dto`) for counter assertions, `log/slog`, swag v2 via `make openapi`.

**Spec:** `docs/plans/2026-09-27-daemon-design-review-2.md` §1 defect list (lines 64-77) and §5 disposition table (lines 291-307); instance behaviour in `docs/plans/2026-10-30-desired-state-protocol.md` §3 (lines 88-93: "`/admin/rules` POST must also use [the single apply path]: drain observation and generation bookkeeping happen once"; "compositions are skipped per rule, counted ... under `category="composition"`"). Research facts (keyed to `develop` paths, moved under `internal/` on this branch): `/private/tmp/claude-501/-Users-pronei-work-faults-lab-atropos-go--claude-worktrees-elated-shannon-771fa0/608f89fc-46c1-49c4-98bd-0b38b054c3ca/scratchpad/research-review-facts.md`.

## Global Constraints

- Branch `feat/atroposd`; worktree `/Users/pronei/work/faults-lab/atropos-go/.claude/worktrees/elated-shannon-771fa0`; run every command from there. Exclude `.claude/` from searches (there is no `vendor/` directory on this branch).
- `gofmt -l .` prints nothing, `go vet ./...` is clean and `go test -count=1 ./...` is green after every task (the full suite includes resource-stress tests and takes a while; the per-package commands below are the fast loop).
- Commit messages are conventional (`fix(instrument): ...`, `feat(interceptor): ...`, `docs: ...`) and end with the trailer line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Metric label for skipped compositions is exactly `atropos_rules_unsupported_total{category="composition"}` (protocol doc §3). The new label key is exactly `atropos.http.status_code`.
- `POST /admin/rules` body stays `[]CompiledRule`; a JSON `null` body clears the set (manteion's teardown push, `rules_admin.go:61-64` on `develop`); `[]` is the same authoritative clear. Unknown fault categories and malformed params still fail the whole set (400 / apply error): only `UnsupportedFaultError` is skipped per rule.
- The push body carries no version: `ManteionClient.ruleVersion` stays owned by `fetchRules` (`internal/instrument/manteion_client.go:258-266`), the only path that receives one. No task touches it.
- HTTP ingress only. The gRPC interceptors (`grpc/interceptor.go:17-26`, `checkAndWait`) and `Interceptor.Hook` (`internal/interceptor/hook.go:15-54`) keep evaluating Ingress/Egress/Custom only; Transient there is out of scope.
- No new dependencies.

## Review Focus

1. Teardown pushes a JSON `null` to `/admin/rules`: the set must clear AND the recording phase's drain report must fire, exactly as an empty polled set does. (Task 1, `TestRulesAdminHandler_PostObservesDrainTracker`.)
2. A composition rule in the middle of a priority-sorted set must not take its neighbours down with it; the counter moves by exactly one per skipped composition. (Task 2, `TestDecodeCompiledRules_CompositionSkippedAndCounted`.)
3. A handler that returns without writing anything: net/http sends 200, so the Transient label must be `200`, not empty. (Task 4, `TestStatusRecorder`.)
4. An ingress inline error fault short-circuits the handler; that is still a completed request, so Transient must see the synthetic status. (Task 4, `TestIngressMiddleware_TransientAfterInlineError`.)
5. An armed `/admin/fault` slot matches every request regardless of point today (`internal/instrument/admin_fault.go:39-53`); at Transient it must not fire, or every inline admin fault starts a second, later-ending fault span per request and `TestServe_BusinessThroughMiddleware_ControlBypassed` (`serve_test.go:168-222`) fails on its span accounting. (Task 4, `TestDemoEvaluator_IgnoresTransientPoint`.)

---

## File structure

| File | Responsibility |
|---|---|
| `internal/instrument/register.go` (modify) | `applyRules`: the single rule-apply function (decode, atomic replace + stop-by-name + eager arm via `applyRuleSet`, drain observation). `apply` delegates its rules branch to it. |
| `internal/instrument/admin_rules.go` (modify) | `rulesAdminHandler(targets applyTargets)`: GET lists, POST normalises `null` to `[]` and calls `applyRules`. `armDecision` skips Transient rules. |
| `internal/instrument/control_surface.go` (modify) | Mounts the handler with `c.targets(cfg.NetworkResolver)`. |
| `internal/instrument/decode.go` (modify) | Composition rules return `*UnsupportedFaultError{Category: CapComposition}` and ride the existing per-rule skip path; generic skip log line. |
| `internal/instrument/capabilities.go` (modify) | `CapComposition = "composition"` label constant. |
| `internal/instrument/admin_fault.go` (modify) | `demoEvaluator.Evaluate` returns nil at Transient. |
| `internal/instrument/middleware.go` (modify) | Uses `interceptor.StatusRecorder`; its private `statusRecorder` is deleted. |
| `internal/interceptor/interceptor.go` (modify) | `StartTransient`; `startRegistered` takes the span-parent context explicitly. |
| `internal/interceptor/middleware.go` (modify) | `IngressMiddleware` wraps the writer, evaluates Transient after the response on both completion paths. |
| `internal/interceptor/response.go` (create) | `StatusRecorder`: shared status-capturing `http.ResponseWriter` wrapper (moved from instrument). |
| `internal/trace/attrs.go` (modify) | `AttrHTTPStatusCode = "atropos.http.status_code"`. |
| `AGENTS.md`, `openapi_docs.go`, `docs/swagger.{yaml,json}` (modify) | Wire-accurate admin tables, Types list, Limitations, Transient status; regenerated OpenAPI. |
| Tests | `internal/instrument/admin_rules_test.go`, `decode_test.go`, `admin_fault_test.go`, `admin_rules_internal_test.go`, `middleware_test.go`; `internal/interceptor/transient_test.go` (new), `interceptor_test.go` (recording tracer captures the start context). |

---

### Task 1: `POST /admin/rules` through the single rule-apply path

**Files:**
- Modify: `internal/instrument/register.go:120-139` (the rules branch of `apply`)
- Modify: `internal/instrument/admin_rules.go:55-100` (`rulesAdminHandler`)
- Modify: `internal/instrument/control_surface.go:48-53`
- Test: `internal/instrument/admin_rules_test.go` (constructor calls at lines 17, 38, 76, 89; two new tests)

**Interfaces:**
- Consumes: `applyTargets` (`register.go:67-93`), `applyRuleSet(reg, eval, rules, arm)` (`admin_rules.go:20`), `decodeCompiledRules` (`decode.go:91`), `cacheDrainTracker.Observe` (`cache_drain.go:82`, nil-safe), `recordingRule(name, experimentID, phaseID)` helper (`cache_drain_test.go:16`).
- Produces: `func applyRules(rules []wire.CompiledRule, targets applyTargets) error` and `func rulesAdminHandler(targets applyTargets) http.Handler`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/instrument/admin_rules_test.go` (add imports `strings`, `sync/atomic`, `git.ucsc.edu/microfaults/atropos-go/internal/cachebox`):

```go
// TestRulesAdminHandler_PostObservesDrainTracker pins the single-apply-path
// contract (design review §5): a rule set pushed to /admin/rules drives the
// drain tracker exactly as a polled set does. Teardown pushes `null`
// (clear everything), which is the common drain trigger.
func TestRulesAdminHandler_PostObservesDrainTracker(t *testing.T) {
	var drains atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/cache/ingest":
			w.WriteHeader(http.StatusCreated)
		case "/api/v1/sdk/cachebox/drain":
			drains.Add(1)
			_ = json.NewEncoder(w).Encode(wire.DrainReportResponse{Accepted: true})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	pusher := newCachePushClient(cachePushConfig{BaseURL: server.URL, Service: "cart", Instance: "pod-1"})
	defer pusher.Stop()
	cb := cachebox.New(cachebox.Config{Push: pusher.PushFunc()})
	defer cb.Stop()
	pusher.BindFidelity(cb.Fidelity())

	eval := evaluator.NewStaticEvaluator()
	handler := rulesAdminHandler(applyTargets{Evaluator: eval, CacheDrain: newCacheDrainTracker(cb, pusher, nil)})
	post := func(body string) int {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", strings.NewReader(body)))
		return rec.Code
	}
	recording, _ := json.Marshal([]wire.CompiledRule{recordingRule("r1", "exp-1", "phase-1")})

	if code := post(string(recording)); code != http.StatusNoContent {
		t.Fatalf("push recording rule: %d", code)
	}
	if got := drains.Load(); got != 0 {
		t.Fatalf("no phase ended yet, got %d drain reports", got)
	}
	// Teardown: manteion pushes null. The set is cleared AND the phase's
	// recording ends, exactly as the empty authoritative set does on poll.
	if code := post("null"); code != http.StatusNoContent {
		t.Fatalf("push null: %d", code)
	}
	if got := len(eval.Rules()); got != 0 {
		t.Fatalf("null push must clear the rule set, still %d rules", got)
	}
	if got := drains.Load(); got != 1 {
		t.Fatalf("null push must fire the drain report once, got %d", got)
	}
	// An explicit empty array is the same authoritative clear.
	if code := post(string(recording)); code != http.StatusNoContent {
		t.Fatalf("re-push recording rule: %d", code)
	}
	if code := post("[]"); code != http.StatusNoContent {
		t.Fatalf("push []: %d", code)
	}
	if got := drains.Load(); got != 2 {
		t.Fatalf("[] push must fire the drain report, got %d in total", got)
	}
}

// TestRulesAdminHandler_PostArmsEagerly: eager arming rides the same apply
// path, so an eagerly-arming host (the sidecar) gets its background faults
// started by a push exactly as by a poll -- once.
func TestRulesAdminHandler_PostArmsEagerly(t *testing.T) {
	var armed []string
	arm := func(point evaluator.InjectionPoint, d *evaluator.Decision) {
		armed = append(armed, point.String()+":"+d.Name)
	}
	eval := evaluator.NewStaticEvaluator()
	handler := rulesAdminHandler(applyTargets{Evaluator: eval, Arm: arm})
	body, _ := json.Marshal([]wire.CompiledRule{{
		Name: "burn", InjectionPoint: "ingress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "resource", FaultType: "cpu", DurationMs: 1000, Params: json.RawMessage(`{"target_load":0.1}`)},
	}})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/rules", bytes.NewReader(body)))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("push: %d %s", rec.Code, rec.Body.String())
	}
	if len(armed) != 1 || armed[0] != "ingress:burn" {
		t.Fatalf("armed = %v, want exactly [ingress:burn]", armed)
	}
}
```

Also change the four existing constructor calls (`admin_rules_test.go:17,38,76,89`) from `rulesAdminHandler(eval, nil, nil)` to `rulesAdminHandler(applyTargets{Evaluator: eval})`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/instrument/ -run 'TestRulesAdminHandler'`
Expected: `FAIL ... [build failed]` with `not enough arguments in call to rulesAdminHandler` / `cannot use applyTargets{…} ... as *evaluator.StaticEvaluator value` (the old signature is `rulesAdminHandler(eval, reg, arm, opts...)`).

- [ ] **Step 3: Extract `applyRules` in `internal/instrument/register.go`**

Add below `apply` (its current rules branch, lines 121-139, moves here verbatim minus the `apply rules:` prefixes):

```go
// applyRules installs an authoritative (non-nil, possibly empty) compiled
// rule set: decode with the targets' options, atomic replace with
// stop-by-name and eager arming (applyRuleSet), then drain observation.
// It is the only rule-application path -- poll and register call it
// through apply, the push endpoint directly -- so those side effects
// happen exactly once per delivery whichever way the set arrives
// (design review §5; desired-state protocol §3).
func applyRules(rules []wire.CompiledRule, targets applyTargets) error {
	if targets.Evaluator == nil && len(rules) > 0 {
		return fmt.Errorf("no Evaluator target for %d rules", len(rules))
	}
	if targets.Evaluator != nil {
		decoded, err := decodeCompiledRules(rules, targets.decodeOpts()...)
		if err != nil {
			return err
		}
		applyRuleSet(targets.Registry, targets.Evaluator, decoded, targets.Arm)
	}
	// Observe every authoritative rule set, INCLUDING empty: the recording
	// phase whose rule drops out most often leaves the set empty, and that
	// transition is exactly what triggers the flush + drain report.
	targets.CacheDrain.Observe(rules)
	return nil
}
```

and replace the rules branch of `apply` (lines 121-139) with:

```go
	if resp.Rules != nil {
		if err := applyRules(resp.Rules, targets); err != nil {
			return fmt.Errorf("apply rules: %w", err)
		}
	}
```

(The "nil = no change, non-nil = authoritative" comment on `apply` stays as is.)

- [ ] **Step 4: Re-point `rulesAdminHandler` at the targets (`internal/instrument/admin_rules.go:55-100`)**

```go
// rulesAdminHandler serves runtime rule management on the host evaluator
// (mounted at /admin/rules). It is the push side of the single rule-apply
// path: POST goes through applyRules exactly as a polled set does, so
// decode, atomic replace, stop-by-name, eager arming and drain observation
// happen once per delivery. The push body carries no version, so the
// poll's ruleVersion (manteion_client.go, fetchRules) is left untouched.
//
// Supported methods:
//   - GET:  200 + JSON-encoded current rule list (empty array if nil)
//   - POST: body is []wire.CompiledRule; null clears the set (teardown);
//     204 on success, 400 on bad JSON or a rule set that does not apply
//   - Other: 405
//
// targets.Evaluator is required.
func rulesAdminHandler(targets applyTargets) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.Method {
		case http.MethodGet:
			rules := targets.Evaluator.Rules()
			if rules == nil {
				rules = []evaluator.StaticRule{}
			}
			json.NewEncoder(w).Encode(rules)

		case http.MethodPost:
			var compiled []wire.CompiledRule
			if err := json.NewDecoder(r.Body).Decode(&compiled); err != nil {
				jsonError(w, fmt.Sprintf("invalid json: %s", err), http.StatusBadRequest)
				return
			}
			if compiled == nil {
				compiled = []wire.CompiledRule{} // null = authoritative clear
			}
			if err := applyRules(compiled, targets); err != nil {
				jsonError(w, fmt.Sprintf("apply rules: %s", err), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			jsonError(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})
}
```

`applyRuleSet`, `armFunc` and `armDecision` (lines 13-53) stay where they are; drop the now-unused `interceptor` import from this file only if nothing else in it uses it (`armDecision` still calls `interceptor.IsInlineFault`, so it stays).

- [ ] **Step 5: Mount with the core's targets (`internal/instrument/control_surface.go:53`)**

```go
	mux.Handle("/admin/rules", rulesAdminHandler(c.targets(cfg.NetworkResolver)))
```

`opts` on line 48 is still used by `faultAdminHandler`; leave it.

- [ ] **Step 6: Run the package tests**

Run: `go test -count=1 ./internal/instrument/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument` (includes `TestRulesAdminHandler_*`, `TestApply_*`, `TestCacheDrainTracker_ObservesPhaseEnd`, `TestApply_UnsupportedActiveFaultDoesNotWedgeSync`, the control-surface tests).

- [ ] **Step 7: Format, vet, full suite**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 8: Commit**

```bash
git add internal/instrument/register.go internal/instrument/admin_rules.go internal/instrument/control_surface.go internal/instrument/admin_rules_test.go
git commit -m "fix(instrument): route POST /admin/rules through the single rule-apply path" -m "applyRules is the one function that decodes, replaces, stops, arms and drain-observes a rule set; apply and the push endpoint both call it, so a pushed set (including teardown's null) fires the cache-box drain report exactly as a polled set does. The push body carries no version, so ruleVersion stays owned by fetchRules." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Composition rules are skipped per rule and counted

**Files:**
- Modify: `internal/instrument/decode.go:101-110` (skip log line) and `:133-148` (composition branch moves before the fault decode and returns the unsupported error)
- Modify: `internal/instrument/capabilities.go:7-12` (`CapComposition`)
- Test: `internal/instrument/decode_test.go:385-411` (replace `TestDecodeCompiledRules_CompositionRejected`)

**Interfaces:**
- Consumes: `UnsupportedFaultError{Category, FaultType}` (`capabilities.go:29-36`), the per-rule skip path in `decodeCompiledRules` (`decode.go:101-110`), `metrics.RulesUnsupportedTotal` (`internal/metrics/metrics.go:83-86`), `unsupportedCount(t, category)` test helper (`capabilities_test.go:22-29`).
- Produces: `const CapComposition = "composition"`; unchanged function signatures.

- [ ] **Step 1: Replace the rejection test with the skip test**

Delete `TestDecodeCompiledRules_CompositionRejected` (`decode_test.go:385-411`) and add:

```go
// TestDecodeCompiledRules_CompositionSkippedAndCounted: a composition rule
// is skipped per rule -- counted, logged -- and never fails the set it
// arrives in (design review §5; protocol §3). Categories keep their
// semantics: see TestDecode_UnsupportedCategoryIsSkippedAndCounted and
// TestDecodeCompiledRules_UnknownCategory.
func TestDecodeCompiledRules_CompositionSkippedAndCounted(t *testing.T) {
	before := unsupportedCount(t, "composition")
	compiled := []wire.CompiledRule{
		{Name: "lat", Mode: "inline", Priority: 10, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
		{Name: "comp-rule", Mode: "inline", Priority: 5, Composition: &wire.CompiledComposition{
			Name: "x", ExecutionMode: "parallel",
			Members: []wire.CompiledCompositionMember{
				{Fault: &wire.CompiledFault{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"10ms"}`)}},
				{Fault: &wire.CompiledFault{Category: "inline", FaultType: "error", Params: json.RawMessage(`{"status_code":500}`)}},
			}}},
		{Name: "err", Mode: "inline", Priority: 1, Fault: &wire.CompiledFault{
			Category: "inline", FaultType: "error", Params: json.RawMessage(`{"status_code":500}`)}},
	}

	rules, err := decodeCompiledRules(compiled)
	if err != nil {
		t.Fatalf("a composition must not fail the set: %v", err)
	}
	if len(rules) != 2 || rules[0].Name != "lat" || rules[1].Name != "err" {
		t.Fatalf("rules = %+v, want [lat err] with the composition skipped", rules)
	}
	if got := unsupportedCount(t, "composition") - before; got != 1 {
		t.Fatalf("atropos_rules_unsupported_total{composition} += %v, want 1", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `go test -count=1 ./internal/instrument/ -run 'TestDecodeCompiledRules_CompositionSkippedAndCounted'`
Expected: `FAIL` with `a composition must not fail the set: rule "comp-rule": composition rules are not yet supported by the SDK evaluator (v6)`.

- [ ] **Step 3: Add the label constant (`internal/instrument/capabilities.go:7-12`)**

```go
	// CapComposition is the atropos_rules_unsupported_total{category} label
	// for composition rules. No host advertises it yet: the decoder skips
	// compositions per rule under this label until manteion expands them
	// into phases (design review §5).
	CapComposition = "composition"
```

- [ ] **Step 4: Return the unsupported error for compositions (`internal/instrument/decode.go:133-148`)**

Move the composition check above the `cr.Fault != nil` block so a composition never decodes a sibling fault first, and make it:

```go
	if cr.Composition != nil {
		// Carried on the wire, not executable here yet (v6: parallel /
		// sequential dispatch with direction inheritance). Skipped per rule
		// like an unsupported category, never failing the set.
		return evaluator.StaticRule{}, &UnsupportedFaultError{Category: CapComposition, FaultType: cr.Composition.ExecutionMode}
	}
```

In `decodeCompiledRules` (`decode.go:107`) make the WARN line category-agnostic:

```go
				slog.Default().Warn("rule skipped: not executable by this host",
					"rule", cr.Name, "category", unsup.Category, "fault_type", unsup.FaultType)
```

The counter increment on line 106 already uses `unsup.Category`, so `category="composition"` needs no further change.

- [ ] **Step 5: Run the decode and capability tests**

Run: `go test -count=1 ./internal/instrument/ -run 'TestDecode|TestApply_UnsupportedActiveFaultDoesNotWedgeSync'`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument` (unknown categories still fail the set: `TestDecodeCompiledRules_UnknownCategory` passes unchanged).

- [ ] **Step 6: Format, vet, full suite**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 7: Commit**

```bash
git add internal/instrument/decode.go internal/instrument/capabilities.go internal/instrument/decode_test.go
git commit -m "fix(instrument): skip composition rules per rule and count them as unsupported" -m "A composition rule now returns UnsupportedFaultError{Category: \"composition\"} and rides the decoder's per-rule skip path: logged at WARN, counted in atropos_rules_unsupported_total{category=\"composition\"}, the rest of the set still applies. Unknown categories and bad params keep failing the set." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `Interceptor.StartTransient`

**Files:**
- Modify: `internal/interceptor/interceptor.go:31-35` (InjectionHook doc), `:112-142` (`StartFault`), `:178-224` (`startRegistered`)
- Modify: `internal/interceptor/interceptor_test.go:342-367` (`recordingTracer` captures the start context)
- Create: `internal/interceptor/transient_test.go`

**Interfaces:**
- Consumes: `FaultRegistry.StartOrJoin` (`registry.go:61-104`; the registry's own context is the unexported field `ctx`, `registry.go:36`), `startDirect` (`interceptor.go:146`), `trace.Tracer` (`internal/trace/trace.go:18-20`).
- Produces: `func (i *Interceptor) StartTransient(ctx context.Context, req evaluator.Request, decision *evaluator.Decision) (CheckResult, error)` — always `Background`, registry-routed when a registry exists (any fault type), span parented on `context.WithoutCancel(ctx)`; `startRegistered(spanParent context.Context, req, decision, faultType, mode)` where `spanParent` is what the fault span is started from (the fault itself always runs under the registry context).

- [ ] **Step 1: Let the recording tracer capture the context it was started with**

In `internal/interceptor/interceptor_test.go` add a field to `recordingTracer` (line 343) and set it in `Start` (line 359):

```go
type recordingTracer struct {
	started  bool
	spanName string
	span     recordingSpan
	// spanCount tracks how many times Start was called (hook + fault = 2).
	spanCount int
	// startCtx is the context the last Start received (span parent).
	startCtx context.Context
}
```

```go
func (t *recordingTracer) Start(ctx context.Context, name string, _ ...attribute.KeyValue) (context.Context, trace.Span) {
	t.started = true
	t.spanCount++
	t.startCtx = ctx
```

- [ ] **Step 2: Write the failing tests**

Create `internal/interceptor/transient_test.go`:

```go
package interceptor

import (
	"context"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/inline"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

func inlineLatencyDecision(name string, d time.Duration) *evaluator.Decision {
	return &evaluator.Decision{Name: name, Reason: "test", Mode: evaluator.Inline,
		Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: d}, Delay: d}}
}

// TestStartTransient_ForcesBackgroundThroughRegistry: at Transient the
// response is already complete, so inline mode is meaningless -- every
// fault, inline types included, runs in Background mode through the
// registry: stoppable by rule name, deduplicated by StartPolicy, reported
// to the injection hook under the "transient" point.
func TestStartTransient_ForcesBackgroundThroughRegistry(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var points []string
	i := New(nil, trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, point string) { points = append(points, point) }))
	req := evaluator.Request{Point: evaluator.Transient}

	cr, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", time.Second))
	if err != nil || cr.Handle == nil {
		t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
	}
	if cr.Decision.Mode != evaluator.Background {
		t.Fatalf("mode = %v, want Background", cr.Decision.Mode)
	}
	if len(points) != 1 || points[0] != "transient" {
		t.Fatalf("injection hook points = %v, want [transient]", points)
	}
	// Registry path: a second start under the same rule joins instead of starting.
	again, err := i.StartTransient(context.Background(), req, inlineLatencyDecision("after", time.Second))
	if err != nil || again.Handle != nil {
		t.Fatalf("second start: handle=%v err=%v, want a deduplicated join", again.Handle, err)
	}
	// Stoppable by rule name, as rule removal requires.
	reg.Stop("after")
	select {
	case <-cr.Handle.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("transient fault did not stop through the registry")
	}
}

// TestStartTransient_OutlivesRequestAndKeepsTraceContext: the fault must not
// die with the request, but its span must still be parented on the request's
// context (values survive context.WithoutCancel; cancellation does not).
// Both the registry and the direct path must behave this way.
func TestStartTransient_OutlivesRequestAndKeepsTraceContext(t *testing.T) {
	type key struct{}
	for _, withRegistry := range []bool{true, false} {
		t.Run(map[bool]string{true: "registry", false: "direct"}[withRegistry], func(t *testing.T) {
			rec := &recordingTracer{}
			var opts []Option
			if withRegistry {
				reg := NewFaultRegistry()
				t.Cleanup(reg.Close)
				opts = append(opts, WithRegistry(reg))
			}
			i := New(nil, rec, opts...)
			reqCtx, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "trace-parent"))

			cr, err := i.StartTransient(reqCtx, evaluator.Request{Point: evaluator.Transient}, inlineLatencyDecision("t", 50*time.Millisecond))
			if err != nil || cr.Handle == nil {
				t.Fatalf("StartTransient: handle=%v err=%v", cr.Handle, err)
			}
			cancel() // the request is gone
			if res := <-cr.Handle.Done(); res.Err != nil {
				t.Fatalf("fault died with the request: %v", res.Err)
			}
			if got := rec.startCtx.Value(key{}); got != "trace-parent" {
				t.Fatalf("span context lost the request's values: got %v", got)
			}
			if rec.startCtx.Done() != nil {
				t.Fatal("span context must be detached from request cancellation")
			}
		})
	}
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/interceptor/ -run 'TestStartTransient'`
Expected: `FAIL ... [build failed]` with `i.StartTransient undefined (type *Interceptor has no field or method StartTransient)`.

- [ ] **Step 4: Implement `StartTransient` and thread the span parent through `startRegistered` (`internal/interceptor/interceptor.go`)**

Update the `InjectionHook` comment (line 31-33) to read `with the fault's Go type and the injection point ("ingress", "egress", "transient" or "custom")`.

Add after `StartFault`:

```go
// StartTransient starts decision's fault for the Transient point, where the
// response is already complete: inline mode has nothing left to block, so
// every fault -- inline types included -- runs in Background mode and, when
// a registry is configured, through it (stoppable by rule name, deduplicated
// by StartPolicy) regardless of its type. The fault span is parented on
// ctx's trace context while the fault runs detached from ctx's cancellation
// (context.WithoutCancel): it outlives the request without losing the link
// to it. An inline error fault here has no response to shape; it only
// leaves its span.
func (i *Interceptor) StartTransient(ctx context.Context, req evaluator.Request, decision *evaluator.Decision) (CheckResult, error) {
	if decision == nil || decision.Fault == nil {
		return CheckResult{}, nil
	}
	if err := decision.Fault.Validate(); err != nil {
		return CheckResult{}, fmt.Errorf("interceptor: invalid fault from evaluator: %w", err)
	}
	faultType := fmt.Sprintf("%T", decision.Fault)
	detached := context.WithoutCancel(ctx)
	if i.registry != nil {
		return i.startRegistered(detached, req, decision, faultType, evaluator.Background)
	}
	return i.startDirect(detached, req, decision, faultType, evaluator.Background)
}
```

In `StartFault` (line 137) pass the registry's own context so ingress/egress background spans stay root spans exactly as today:

```go
	if i.registry != nil && !IsInlineFault(decision.Fault) {
		return i.startRegistered(i.registry.ctx, req, decision, faultType, effectiveMode)
	}
```

In `startRegistered` rename the first parameter to `spanParent` and start the span from it instead of `regCtx` (line 188); the fault still starts with `regCtx` (line 201). New doc comment:

```go
// startRegistered routes faults through the registry for deduplication and
// detachment: the fault runs under the registry context so it can outlive
// the triggering request and be stopped by key. Its span is parented on
// spanParent -- the registry context for StartFault (a root span, as
// before) or the request context stripped of cancellation for
// StartTransient (a child of the request's server span).
```

- [ ] **Step 5: Run the interceptor tests**

Run: `go test -count=1 ./internal/interceptor/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/interceptor`.

- [ ] **Step 6: Format, vet, full suite**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 7: Commit**

```bash
git add internal/interceptor/interceptor.go internal/interceptor/interceptor_test.go internal/interceptor/transient_test.go
git commit -m "feat(interceptor): StartTransient runs a detached background fault parented on the request trace" -m "At the Transient point the response is complete, so every fault is forced to Background mode and routed through the registry regardless of type; the span parent is context.WithoutCancel of the request context, so the fault outlives the request as a child span of it. Ingress/egress background starts keep their root spans." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Evaluate the Transient point in the ingress middleware

**Files:**
- Create: `internal/interceptor/response.go` (`StatusRecorder`, moved from `internal/instrument/middleware.go:94-124`)
- Modify: `internal/interceptor/middleware.go:27-54` (`IngressMiddleware`) plus a `transient` helper
- Modify: `internal/trace/attrs.go:16-21` (`AttrHTTPStatusCode`)
- Modify: `internal/instrument/middleware.go:15-34` and `:94-124`
- Modify: `internal/instrument/admin_fault.go:39-53` (`demoEvaluator.Evaluate`)
- Modify: `internal/instrument/admin_rules.go:46-53` (`armDecision`)
- Test: `internal/interceptor/transient_test.go` (extend), `internal/instrument/middleware_test.go`, `internal/instrument/admin_fault_test.go`, `internal/instrument/admin_rules_internal_test.go`

**Interfaces:**
- Consumes: `Interceptor.StartTransient` (Task 3), `Interceptor.Evaluate` (`interceptor.go:97`), `extractHTTPLabels` (`middleware.go:131`), `StaticEvaluator` point filtering (`internal/evaluator/static.go:45-49`).
- Produces: `type StatusRecorder struct` with `NewStatusRecorder(w http.ResponseWriter) *StatusRecorder`, `Status() int` (200 until the first `WriteHeader`/`Write`), `WriteHeader`, `Write`, `Flush`, `Unwrap`; `const trace.AttrHTTPStatusCode = "atropos.http.status_code"`; `func (i *Interceptor) transient(r *http.Request, labels map[string]string, status int)` (unexported).

- [ ] **Step 1: Write the failing middleware tests**

Append to `internal/interceptor/transient_test.go` (add imports `errors`, `net/http`, `net/http/httptest`, `strconv`, `sync/atomic`):

```go
// transientRule matches the Transient point with the given labels.
func transientRule(name string, labels map[string]string, f fault.Fault) evaluator.StaticRule {
	return evaluator.StaticRule{Name: name, Point: evaluator.Transient, Labels: labels,
		Decision: evaluator.Decision{Name: name, Reason: "test", Mode: evaluator.Inline, Fault: f}}
}

func shortLatency() fault.Fault {
	return &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}
}

// afterHandlerFault records whether the handler had completed when Start ran.
type afterHandlerFault struct {
	handlerDone  *atomic.Bool
	startedAfter atomic.Bool
	started      chan struct{}
}

func (f *afterHandlerFault) Validate() error { return nil }
func (f *afterHandlerFault) Start(ctx context.Context) (*fault.Handle, error) {
	f.startedAfter.Store(f.handlerDone.Load())
	_, cancel := context.WithCancel(ctx)
	h := fault.NewHandle(cancel)
	go func() { defer cancel(); h.Send(fault.Result{}) }()
	close(f.started)
	return h, nil
}

// TestIngressMiddleware_TransientFiresAfterResponse: the Transient point is
// evaluated once the handler has written its response, with the response
// status as a label, and the fault starts under the "transient" point.
func TestIngressMiddleware_TransientFiresAfterResponse(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var handlerDone atomic.Bool
	f := &afterHandlerFault{handlerDone: &handlerDone, started: make(chan struct{})}
	var points []string
	i := New(evaluator.NewStaticEvaluator(transientRule("after", map[string]string{trace.AttrHTTPStatusCode: "201"}, f)),
		trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, point string) { points = append(points, point) }))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("made"))
		handlerDone.Store(true)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("POST", "/orders", nil))
	if rec.Code != http.StatusCreated || rec.Body.String() != "made" {
		t.Fatalf("response = %d %q, want 201 made", rec.Code, rec.Body.String())
	}
	select {
	case <-f.started:
	case <-time.After(time.Second):
		t.Fatal("transient fault did not start")
	}
	if !f.startedAfter.Load() {
		t.Fatal("transient fault started before the handler finished writing")
	}
	if len(points) != 1 || points[0] != "transient" {
		t.Fatalf("injection hook points = %v, want [transient]", points)
	}
}

// TestIngressMiddleware_TransientDoesNotDelayResponse: a rule authored in
// inline mode still runs in the background at Transient, tracked under its
// rule name.
func TestIngressMiddleware_TransientDoesNotDelayResponse(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	slow := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: 300 * time.Millisecond}, Delay: 300 * time.Millisecond}
	i := New(evaluator.NewStaticEvaluator(transientRule("slow", nil, slow)), trace.Noop(), WithRegistry(reg))
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }))

	start := time.Now()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("response took %v; the transient fault must not block it", elapsed)
	}
	startedAnother := false
	_, deduped, err := reg.StartOrJoin("slow", evaluator.DeduplicateByRule, func(context.Context) (*fault.Handle, error) {
		startedAnother = true
		return nil, errors.New("unexpected start")
	})
	if startedAnother || !deduped || err != nil {
		t.Fatalf("startedAnother=%v deduped=%v err=%v, want a join on the running transient fault", startedAnother, deduped, err)
	}
	reg.Stop("slow")
}

// TestIngressMiddleware_TransientMatchesResponseStatus: the status label is
// the response's status; a rule for another status does not fire.
func TestIngressMiddleware_TransientMatchesResponseStatus(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   bool
	}{{503, true}, {200, false}} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			reg := NewFaultRegistry()
			t.Cleanup(reg.Close)
			var fired atomic.Int32
			i := New(evaluator.NewStaticEvaluator(transientRule("on-503", map[string]string{trace.AttrHTTPStatusCode: "503"}, shortLatency())),
				trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, _ string) { fired.Add(1) }))
			h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/x", nil))
			if got := fired.Load() == 1; got != tc.want {
				t.Fatalf("status %d: fired=%v, want %v", tc.status, got, tc.want)
			}
		})
	}
}

// TestIngressMiddleware_TransientAfterInlineError: an injected error
// response is a completed request too; Transient sees its status and the
// handler still never runs.
func TestIngressMiddleware_TransientAfterInlineError(t *testing.T) {
	reg := NewFaultRegistry()
	t.Cleanup(reg.Close)
	var points []string
	i := New(evaluator.NewStaticEvaluator(
		evaluator.StaticRule{Name: "err", Point: evaluator.Ingress, Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: 503, Message: "down"}}},
		transientRule("after-503", map[string]string{trace.AttrHTTPStatusCode: "503"}, shortLatency()),
	), trace.Noop(), WithRegistry(reg), WithInjectionHook(func(_, point string) { points = append(points, point) }))
	var reached atomic.Bool
	h := i.IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 503 || reached.Load() {
		t.Fatalf("code=%d reached=%v, want the injected 503 without the handler", rec.Code, reached.Load())
	}
	if len(points) != 2 || points[0] != "ingress" || points[1] != "transient" {
		t.Fatalf("points = %v, want [ingress transient]", points)
	}
}

// TestStatusRecorder pins the status the Transient label is built from: 200
// until the handler writes, the first WriteHeader wins, a bare Write is 200,
// and Unwrap exposes the underlying writer for http.ResponseController.
func TestStatusRecorder(t *testing.T) {
	w := httptest.NewRecorder()
	r := NewStatusRecorder(w)
	if r.Status() != 200 {
		t.Fatalf("default status = %d, want 200", r.Status())
	}
	r.WriteHeader(404)
	r.WriteHeader(500)
	if r.Status() != 404 || w.Code != 404 {
		t.Fatalf("status = %d (underlying %d), want the first WriteHeader (404)", r.Status(), w.Code)
	}
	bare := NewStatusRecorder(httptest.NewRecorder())
	_, _ = bare.Write([]byte("x"))
	if bare.Status() != 200 {
		t.Fatalf("status after bare Write = %d, want 200", bare.Status())
	}
	if r.Unwrap() != w {
		t.Fatal("Unwrap must return the underlying writer")
	}
}
```

Append to `internal/instrument/middleware_test.go` (add imports `time`, `git.ucsc.edu/microfaults/atropos-go/internal/trace`):

```go
// TestIngressMiddleware_TransientThroughFullComposition: metrics + otelhttp +
// fault middleware; the transient rule sees the status the handler wrote and
// the injection hook is called with the transient point.
func TestIngressMiddleware_TransientThroughFullComposition(t *testing.T) {
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	var points []string
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "after-404", Point: evaluator.Transient, Labels: map[string]string{trace.AttrHTTPStatusCode: "404"},
		Decision: evaluator.Decision{Name: "after-404", Mode: evaluator.Inline, Reason: "test",
			Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}},
	}), nil, interceptor.WithRegistry(reg), interceptor.WithInjectionHook(func(_, p string) { points = append(points, p) }))
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) }), "svc", ic)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/missing", nil))
	if rec.Code != 404 {
		t.Fatalf("code = %d, want 404", rec.Code)
	}
	if len(points) != 1 || points[0] != "transient" {
		t.Fatalf("injection points = %v, want [transient]", points)
	}
}
```

Append to `internal/instrument/admin_fault_test.go` (add import `git.ucsc.edu/microfaults/atropos-go/internal/fault/inline`; `context`, `time`, `fault`, `evaluator`, `wire` are already imported):

```go
// TestDemoEvaluator_IgnoresTransientPoint: an armed admin fault matches the
// request path (ingress/egress/custom), never the post-response point --
// otherwise every armed inline fault would fire twice per request and the
// span accounting in TestServe_BusinessThroughMiddleware_ControlBypassed
// would see the second, later-ending fault span.
func TestDemoEvaluator_IgnoresTransientPoint(t *testing.T) {
	demo := &demoEvaluator{}
	demo.Set(&evaluator.Decision{Name: "lat", Reason: "admin", Mode: evaluator.Inline,
		Fault: &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Millisecond}, Delay: time.Millisecond}},
		&wire.FaultRequest{ID: "lat", Category: "inline", FaultType: "latency"})
	if demo.Evaluate(context.Background(), evaluator.Request{Point: evaluator.Ingress}) == nil {
		t.Fatal("armed slot must match at ingress")
	}
	if d := demo.Evaluate(context.Background(), evaluator.Request{Point: evaluator.Transient}); d != nil {
		t.Fatalf("armed slot matched at Transient: %+v", d)
	}
}
```

Append to `internal/instrument/admin_rules_internal_test.go` (add import `git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/cpu`):

```go
// TestArmDecision_SkipsTransientRules: a Transient rule is request-triggered
// by definition (its status label only exists once a response completed),
// so eagerly-arming hosts must not start it at apply time.
func TestArmDecision_SkipsTransientRules(t *testing.T) {
	var armed int
	arm := func(evaluator.InjectionPoint, *evaluator.Decision) { armed++ }
	d := &evaluator.Decision{Name: "burn", Fault: &cpu.Stress{}}
	armDecision(arm, evaluator.Ingress, d)
	armDecision(arm, evaluator.Transient, d)
	if armed != 1 {
		t.Fatalf("armed %d times, want 1 (ingress only)", armed)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./internal/interceptor/ -run 'TestIngressMiddleware_Transient|TestStatusRecorder'`
Expected: `FAIL ... [build failed]` (`undefined: trace.AttrHTTPStatusCode`, `undefined: NewStatusRecorder`).

Run: `go test -count=1 ./internal/instrument/ -run 'TestDemoEvaluator_IgnoresTransientPoint|TestArmDecision_SkipsTransientRules'`
Expected: `FAIL` with `armed slot matched at Transient` and `armed 2 times, want 1`.

- [ ] **Step 3: Add the label key (`internal/trace/attrs.go:16-21`)**

```go
	AttrHTTPStatusCode = "atropos.http.status_code" // Transient point only: the completed response's status
```

- [ ] **Step 4: Create `internal/interceptor/response.go`**

Move `statusRecorder` from `internal/instrument/middleware.go:94-124` here, exported, with a constructor and an accessor; `WriteHeader`, `Write`, `Flush` and `Unwrap` keep their bodies:

```go
package interceptor

import "net/http"

// StatusRecorder is the ResponseWriter wrapper the ingress layers use to
// learn the status a handler wrote: the metrics layer labels its series with
// it and the fault middleware feeds it to the Transient point. The status is
// 200 until the first WriteHeader or Write (what net/http sends for a
// handler that writes nothing). Flush is forwarded and Unwrap exposes the
// underlying writer so http.ResponseController reaches Hijack and friends.
type StatusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

// NewStatusRecorder wraps w.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Status is the recorded status code.
func (r *StatusRecorder) Status() int { return r.status }
```

- [ ] **Step 5: Evaluate Transient after the response (`internal/interceptor/middleware.go:27-54`)**

Add imports `maps` and `strconv`. Replace `IngressMiddleware` and add the helper:

```go
// IngressMiddleware checks for faults on inbound HTTP requests, then -- once
// the response is complete, whichever path produced it -- evaluates the
// Transient point with the same labels plus the response status.
// Cache-box is not dispatched at ingress in Stage 1 -- see
// internal/interceptor/cachebox.go for the egress dispatcher.
func (i *Interceptor) IngressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		labels := extractHTTPLabels(r)
		cr, err := i.Check(r.Context(), evaluator.Request{Point: evaluator.Ingress, Labels: labels})
		if err != nil {
			log.Printf("atropos: ingress check error: %v", err)
		}

		rec := NewStatusRecorder(w)
		if cr.Handle != nil && cr.Decision.Mode == evaluator.Inline {
			res := <-cr.Handle.Done()
			// An inline error fault is the response: the handler never runs.
			if d := inlinefault.IsErrorResult(res); d != nil {
				rec.Header().Set(HeaderFault, "error")
				http.Error(rec, d.StatusText(), d.StatusCode)
				i.transient(r, labels, rec.Status())
				return
			}
		}

		next.ServeHTTP(rec, r)
		i.transient(r, labels, rec.Status())
	})
}

// transient evaluates the Transient point for a completed response: the
// ingress labels plus atropos.http.status_code. A matching fault starts
// detached from the request and never blocks it (StartTransient); cache-box
// decisions have no meaning here and are ignored.
func (i *Interceptor) transient(r *http.Request, labels map[string]string, status int) {
	tl := maps.Clone(labels)
	tl[trace.AttrHTTPStatusCode] = strconv.Itoa(status)
	req := evaluator.Request{Point: evaluator.Transient, Labels: tl}
	d := i.Evaluate(r.Context(), req)
	if d == nil || d.CacheBox != evaluator.CacheBoxNone {
		return
	}
	if _, err := i.StartTransient(r.Context(), req, d); err != nil {
		log.Printf("atropos: transient check error: %v", err)
	}
}
```

- [ ] **Step 6: Use the shared recorder in `internal/instrument/middleware.go`**

Line 24 becomes `rec := interceptor.NewStatusRecorder(w)`, line 30 `status := strconv.Itoa(rec.Status())`; delete the `statusRecorder` type and methods (lines 94-124). Extend the doc comment (lines 15-19): "... the interceptor's fault check (inner, so the fault span is a child of the request span; it also evaluates the Transient point once the response is complete, so a transient fault span is a child of the request span too)."

- [ ] **Step 7: Keep admin slots and eager arming off the Transient point**

`internal/instrument/admin_fault.go:39` — first lines of `Evaluate`:

```go
func (d *demoEvaluator) Evaluate(_ context.Context, req evaluator.Request) *evaluator.Decision {
	// Admin slots arm the request path. The post-response point would fire
	// every armed inline fault a second time per request.
	if req.Point == evaluator.Transient {
		return nil
	}
```

`internal/instrument/admin_rules.go:48` — `armDecision`:

```go
func armDecision(arm armFunc, point evaluator.InjectionPoint, d *evaluator.Decision) {
	// Transient rules are request-triggered by definition: their status
	// label only exists once a response has completed.
	if arm == nil || d == nil || d.Fault == nil || point == evaluator.Transient || interceptor.IsInlineFault(d.Fault) {
		return
	}
	arm(point, d)
}
```

- [ ] **Step 8: Run the touched packages**

Run: `go test -count=1 ./internal/interceptor/ ./internal/instrument/ ./internal/trace/`
Expected: three `ok` lines.

- [ ] **Step 9: Format, vet, full suite (the sidecar proxies and the root package exercise this middleware)**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: no gofmt output, no vet output, every package `ok` — including `internal/proxy`, `internal/atroposd` and the root package (`TestServe_BusinessThroughMiddleware_ControlBypassed` relies on Step 7's demo guard; the always-match evaluators in `middleware_test.go:21-27` and `integration_test.go:78-84` now also match at Transient and start one extra detached 30-50 ms latency fault, which none of their assertions observe).

- [ ] **Step 10: Commit**

```bash
git add internal/interceptor/response.go internal/interceptor/middleware.go internal/interceptor/transient_test.go internal/trace/attrs.go internal/instrument/middleware.go internal/instrument/middleware_test.go internal/instrument/admin_fault.go internal/instrument/admin_fault_test.go internal/instrument/admin_rules.go internal/instrument/admin_rules_internal_test.go
git commit -m "feat(interceptor): evaluate the Transient injection point after the response completes" -m "The ingress middleware wraps the writer and, once the response is complete (handler returned or an inline error short-circuited it), evaluates Transient with the ingress labels plus atropos.http.status_code; the matching fault starts through StartTransient, never delaying the response. Admin slots and eager arming skip the point. The status recorder moves to the interceptor package so both ingress layers share it. HTTP only; gRPC and hooks are unchanged." -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `AGENTS.md` and OpenAPI drift

**Files:**
- Modify: `AGENTS.md:86-90` (cachebox table), `:92-99` (rules table), `:110-115` (Types), `:137-142` (Limitations)
- Modify: `openapi_docs.go:150-160` (POST body type), regenerate `docs/swagger.yaml`, `docs/swagger.json` (`make openapi`, swag v2 is on PATH at `/Users/pronei/go/bin/swag`)

**Interfaces:**
- Consumes: the behaviour shipped by Tasks 1-4; `wire.RegisterRequest` (`internal/wire/register.go:4-20`), `wire.RegisterResponse`/`wire.RuleSync` (`register.go:26-29`, `rules.go:75-80`), `wire.DelayRequest` (`internal/wire/cachebox.go:33-38`), root aliases `CompiledRule`/`StaticRule` (`types.go:27,33`).
- Produces: documentation only.

- [ ] **Step 1: Rewrite the `/admin/rules` section (`AGENTS.md:92-99`)**

```markdown
### /admin/rules

Runtime rule-set management against the host evaluator.

| Method | Path           | Body                                            | Response |
|--------|----------------|-------------------------------------------------|----------|
| GET    | `/admin/rules` | —                                               | 200 `[]StaticRule` JSON (the installed, decoded rules) |
| POST   | `/admin/rules` | `[]CompiledRule` JSON; `null` clears the set    | 204 (atomic replace) or 400 `{"error": ...}` |

POST is the push side of the single rule-apply path the manteion sync uses
(`applyRules` in `internal/instrument/register.go`): decode against this
host's capabilities, atomic replace, stop of background faults whose rule
vanished, eager arming (sidecar) and cache-box drain observation happen
exactly once per delivery. Rules this host cannot execute — categories
outside its capabilities and composition rules — are skipped per rule,
logged at WARN and counted in `atropos_rules_unsupported_total{category}`
(`category="composition"` for compositions); the rest of the set still
applies. Unknown categories and malformed params reject the whole set with
400. The push body carries no version, so the poll's rule version is left
untouched.
```

- [ ] **Step 2: Fix the cache-box delay row (`AGENTS.md:89`)**

```markdown
| POST   | `/admin/cachebox/delay` | `{"mu": float, "sigma": float, "seed"?: uint64, "context"?: CacheBoxContext}` | 204 (replaces delay source with lognormal distribution; `context` is accepted on the wire, not yet honoured) |
```

- [ ] **Step 3: Rewrite the Types list (`AGENTS.md:112-115`)**

```markdown
- `RegisterRequest{ID, Service, Version, Address, PollIntervalMs, Routes, Host, Capabilities}` — the POST body. `Routes` is the optional HTTP route inventory for manteion's workflow-builder catalog; `Host` (`in-process` | `sidecar`) and `Capabilities` (the fault categories this host executes) are additive.
- `Route{Method, Path, Description, DependsOn}` — one published HTTP route. `DependsOn` entries are `"METHOD /path"` (same service) or `"service METHOD /path"` (fully qualified).
- `RegisterResponse{Status, RuleSync}` — the response embeds `RuleSync{Version, Rules, ActiveFaults, FreezeCfg}`, the same desired-state payload `GET /api/v1/sdk/rules` serves, so the SDK converges before its first poll. `Rules` and `ActiveFaults` are never omitted: `[]` clears the category, a `null`/absent `rules` means no change.
- `CompiledRule`, `CompiledFault`, `CompiledComposition`, `CompiledCompositionMember` — the JSON wire format for rules, mirroring `manteion-go/internal/ruleconv`. `CompiledComposition` is carried on the wire but not yet executable on the SDK side; the decoder skips composition rules per rule and counts them (see `/admin/rules`).
```

- [ ] **Step 4: Update Limitations (`AGENTS.md:137-142`)**

Replace line 141 with:

```markdown
- Composition rules are skipped per rule on decode (logged at WARN, counted under `atropos_rules_unsupported_total{category="composition"}`) — the SDK has no composition evaluator yet (parallel/sequential fault dispatch with direction inheritance is future work).
```

and append:

```markdown
- Transient (taxonomy 1.3) is implemented for HTTP ingress. Once the response is complete — the handler returned, or an inline error fault short-circuited it — the Transient point is evaluated with the ingress labels plus `atropos.http.status_code`. A matching fault always runs in background mode through the fault registry (inline mode is meaningless after the response and is not honoured), detached from the request's cancellation but with its span parented on the request's server span. Armed `/admin/fault` slots and eager arming do not apply at Transient. The gRPC interceptors and `Hook` do not evaluate it.
```

- [ ] **Step 5: Fix the OpenAPI annotation and regenerate**

`openapi_docs.go:156`:

```go
// @Param    rules  body  []CompiledRule  true  "replacement rules (null clears the set)"
```

Run: `make openapi && git diff --stat docs/swagger.yaml docs/swagger.json`
Expected: swag exits 0; the diff touches the `/admin/rules` POST request body (now `atropos.CompiledRule` items) and nothing else of substance.

- [ ] **Step 6: Verify the gates still hold**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./...`
Expected: no gofmt output, no vet output, every package `ok`.

- [ ] **Step 7: Commit**

```bash
git add AGENTS.md openapi_docs.go docs/swagger.yaml docs/swagger.json
git commit -m "docs: AGENTS.md admin tables, RegisterResponse/RuleSync types and the Transient point; OpenAPI body for POST /admin/rules" -m "Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

- **Spec coverage.** Scope 1 (push through the apply path: drain, arm, stop once; version untouched because the push carries none) → Task 1. Scope 2 (compositions skipped per rule, WARN, `category="composition"`, categories unchanged) → Task 2. Scope 3 (Transient after the response, labels + `atropos.http.status_code`, background through the registry, `context.WithoutCancel`, inline forced to background, HTTP only) → Tasks 3-4; gRPC/hooks noted in Global Constraints and `AGENTS.md`. Scope 4 (`AGENTS.md` tables, Types, Transient status) → Task 5, plus the identical drift in `openapi_docs.go`.
- **Step scan.** Every code step names the file, line range and signature; bodies appear where the user asked for real code or where a move/relocation fixes them. Each test step carries the assertions; each run step the command and the expected output.
- **Type consistency.** `applyRules(rules []wire.CompiledRule, targets applyTargets) error` and `rulesAdminHandler(targets applyTargets)` (Task 1) match the control-surface call; `StartTransient(ctx, req, decision) (CheckResult, error)` (Task 3) matches `transient` (Task 4); `startRegistered(spanParent, ...)` is called with `i.registry.ctx` from `StartFault` and with `context.WithoutCancel(ctx)` from `StartTransient`; `NewStatusRecorder`/`Status()` are used identically in both middleware layers; `CapComposition` is the label the test reads back.
- **Review Focus.** All five lines have a named test in the owning task.
- **Proportion.** Four small production changes and one doc task; the test code is the bulk of the document by design (the scope asked for real code in every code step).

## Decisions taken here that the reviewer may want to veto

1. **Admin slots never match at Transient** (Task 4 Step 7). Required for the existing root-package span-accounting test, and it keeps `/admin/fault` meaning "fires on the request path".
2. **Transient rules are excluded from eager arming** (Task 4 Step 7). Eager arming ignores labels; a transient rule's label is the status of a response that has not happened yet.
3. **Ingress/egress background faults keep root spans.** `StartFault` passes the registry context to `startRegistered`; switching that one argument to `context.WithoutCancel(ctx)` would make every background fault a child of its triggering request, which is ADR-3's correlation goal but outside this scope.
4. **The push does not touch `ruleVersion`.** Resetting it to 0 would force the next poll to replace the pushed phase rules with manteion's poll snapshot (today's two truths, ADR-2's problem). Left as is; `fetchRules` stays the only writer.
5. **Transient fires after an inline error short-circuit too.** "The original request has completed" includes a synthesized response; the test pins it so a reviewer who disagrees sees exactly where to flip it.
