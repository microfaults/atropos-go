# atroposd M0: Core Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the root `atropos` package into an importable instrument core (`internal/instrument`), a wire-type package (`internal/wire`), and a metrics package (`internal/metrics`), so `atropos.Serve` becomes a thin in-process host and a second host (`cmd/atroposd`, M1) can be built on the same core. **No behavior change.**

**Architecture:** The root package today is simultaneously the wire-type package manteion imports, the in-process host, and the instrument core, with the core unexported. After M0: `internal/wire` holds every JSON wire struct (root keeps type aliases with identical names so manteion's `atroposdk.X` and service-beds' `atropos.X` keep compiling); `internal/metrics` holds the Prometheus registry and series; `internal/instrument` holds the core (evaluators, cache-box, push, drain, fault registry, interceptor), the manteion client, the rule decoders, and the control-surface handlers. Root keeps `Serve`, `Config`, `EgressTransport`, `DefaultInterceptor`, `Health`, the middleware, and the aliases.

**Tech Stack:** Go 1.25 module `git.ucsc.edu/microfaults/atropos-go`; stdlib `testing`; Prometheus client; OTel SDK; `swag` v2 for the OpenAPI check (`make openapi-check`).

**Invariants this plan must keep (verified by the existing suite):**
- Every exported root name listed in `go doc -all .` at `de80d5e` still exists with the same JSON tags (wire_fidelity_test.go, wire_compat_test.go).
- The fault registry is process-lifetime for the in-process host and never swapped (A1): root keeps `defaultRegistry` and hands it to the core.
- Control paths are mounted outside the middleware; host rules win over the admin slot; offline mode works; configured-but-unreachable manteion is fatal (serve_test.go).

---

## File structure after M0

```
atropos-go/
├── atropos.go            defaultInterceptor/defaultRegistry globals, DefaultInterceptor, configure (tests)
├── serve.go              Config, ShutdownFunc, Serve — thin host over internal/instrument
├── middleware.go         ingressMiddleware, EgressTransport (unchanged logic; metrics via internal/metrics)
├── health.go             Health() over the root globalClient (HealthStatus is an alias)
├── types.go              ALL root type aliases → internal/wire, internal/cachebox, internal/evaluator, internal/interceptor
├── openapi_docs.go       unchanged (annotations reference the aliases)
├── internal/metrics/metrics.go          Registry, series, RecordFaultInjection, Handler
├── internal/wire/{doc,fault,rules,register,cachebox,health,errors,resolver}.go
└── internal/instrument/
    ├── core.go           CoreConfig, Core, NewCore, Targets, StopFaults, Stop
    ├── identity.go       ResolveInstanceID
    ├── control_surface.go ControlConfig, ControlSurface
    ├── telemetry.go      InitTelemetry, TelemetryConfig                (moved)
    ├── manteion.go / manteion_client.go  ConnectManteion, ManteionConfig, ManteionClient (moved)
    ├── register.go       RegisterWith, ApplyTargets, Apply, StartFaultWatchdog (moved)
    ├── decode.go         DecodeCompiledRules, DecodeOption, WithNetworkResolver, BuildFault (moved from compiled_rule.go)
    ├── admin_fault.go    DemoEvaluator, FaultAdminHandler              (moved from admin.go)
    ├── admin_rules.go    RulesAdminHandler, applyRuleSet               (moved from rules_admin.go)
    ├── admin_cachebox.go CacheBoxAdminHandler                          (moved)
    ├── preload.go        CacheBoxPreloadHandler                        (moved from cachebox_preload.go)
    ├── fidelity_handler.go CacheBoxFidelityHandler                     (moved)
    ├── health.go         HealthHandler, HealthFrom                     (moved handler half)
    ├── cache_push.go     CachePushClient, CachePushConfig, NewCachePushClient (moved)
    ├── cache_drain.go    CacheDrainTracker, NewCacheDrainTracker       (moved)
    └── errors.go         jsonError                                     (moved)
```

Tests move with their files (package `instrument`). Tests that stay in root: `serve_test.go`, `middleware_test.go`, `integration_test.go`, `metrics_test.go`, `wire_fidelity_test.go`.

---

### Task 1: `internal/metrics` package

**Files:**
- Create: `internal/metrics/metrics.go`
- Modify: `middleware.go`
- Modify: `metrics_test.go`
- Delete: `metrics.go`

- [ ] **Step 1: Create the package with the exact series definitions moved from root `metrics.go`**

```go
// Package metrics owns the atropos Prometheus registry and the series the
// HTTP middleware, the egress transport, the interceptor injection hook and
// (from M1) the daemon's proxy listeners record into. It is a leaf package so
// every host of the instrument core shares one set of series definitions.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds the atropos-specific series. Handler merges it with the
// default Go runtime/process gatherer.
var Registry = prometheus.NewRegistry()

var (
	HTTPServerRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_server_request_duration_seconds",
		Help:    "Duration of HTTP server requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "service"})

	HTTPServerRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_server_requests_total",
		Help: "Total HTTP server requests.",
	}, []string{"method", "status_code", "service"})

	HTTPClientRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_client_request_duration_seconds",
		Help:    "Duration of HTTP client requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "status_code", "target"})

	HTTPClientRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_client_requests_total",
		Help: "Total HTTP client requests.",
	}, []string{"method", "status_code", "target"})

	CacheBoxHitsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_hits_total",
		Help: "Total cache-box hits (replay served from cache).",
	})

	CacheBoxMissesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_misses_total",
		Help: "Total cache-box misses (fell back to passthrough).",
	})

	CacheBoxRecordsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "atropos_cachebox_records_total",
		Help: "Total cache-box records (passthrough responses captured).",
	})

	// FaultInjectionsTotal counts every fault the interceptor actually
	// starts, labelled by fault type and injection point.
	FaultInjectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_fault_injections_total",
		Help: "Total faults started by the interceptor, by type and injection point.",
	}, []string{"fault_type", "injection_point"})
)

func init() {
	Registry.MustRegister(
		HTTPServerRequestDuration,
		HTTPServerRequestsTotal,
		HTTPClientRequestDuration,
		HTTPClientRequestsTotal,
		CacheBoxHitsTotal,
		CacheBoxMissesTotal,
		CacheBoxRecordsTotal,
		FaultInjectionsTotal,
	)
}

// RecordFaultInjection is the interceptor.InjectionHook every host installs.
func RecordFaultInjection(faultType, injectionPoint string) {
	FaultInjectionsTotal.WithLabelValues(faultType, injectionPoint).Inc()
}

// Handler serves Prometheus metrics: the atropos series plus the default Go
// runtime/process series.
func Handler() http.Handler {
	return promhttp.HandlerFor(
		prometheus.Gatherers{Registry, prometheus.DefaultGatherer},
		promhttp.HandlerOpts{},
	)
}
```

- [ ] **Step 2: Point root `middleware.go` at the package**

Add the import `"git.ucsc.edu/microfaults/atropos-go/internal/metrics"` and rename every series reference: `httpServerRequestDuration` → `metrics.HTTPServerRequestDuration`, `httpServerRequestsTotal` → `metrics.HTTPServerRequestsTotal`, `httpClientRequestDuration` → `metrics.HTTPClientRequestDuration`, `httpClientRequestsTotal` → `metrics.HTTPClientRequestsTotal`, `cacheBoxMissesTotal` → `metrics.CacheBoxMissesTotal`, `cacheBoxHitsTotal` → `metrics.CacheBoxHitsTotal`, `cacheBoxRecordsTotal` → `metrics.CacheBoxRecordsTotal`.

- [ ] **Step 3: Point `atropos.go` at the hook and delete root `metrics.go`**

In `atropos.go`, both `interceptor.WithInjectionHook(recordFaultInjection)` occurrences become `interceptor.WithInjectionHook(metrics.RecordFaultInjection)` (add the import). Then `git rm metrics.go`.

- [ ] **Step 4: Adapt `metrics_test.go`**

`atroposRegistry.Gather()` → `metrics.Registry.Gather()` (two sites); `metricsHandler()` → `metrics.Handler()`; add the import. The test stays in package `atropos` because it also drives `ingressMiddleware`.

- [ ] **Step 5: Build and test**

Run: `go build ./... && go vet ./... && go test -count=1 . ./internal/metrics/`
Expected: `ok  git.ucsc.edu/microfaults/atropos-go` (no failures; `internal/metrics` has no test files).

- [ ] **Step 6: Commit**

```bash
git add internal/metrics/metrics.go middleware.go atropos.go metrics_test.go metrics.go
git commit -m "refactor(m0): move Prometheus series into internal/metrics"
```

---

### Task 2: `internal/wire` package + root aliases

**Files:**
- Create: `internal/wire/doc.go`, `internal/wire/fault.go`, `internal/wire/rules.go`, `internal/wire/register.go`, `internal/wire/cachebox.go`, `internal/wire/health.go`, `internal/wire/errors.go`, `internal/wire/resolver.go`
- Modify: `types.go` (becomes the alias file), `admin.go`, `compiled_rule.go`, `register.go`, `cachebox_admin.go`, `cachebox_fidelity.go`, `health.go`, `errors.go`
- Delete: `sync.go`, `routes.go`, `cachebox_fidelity.go` (types only; `CacheBoxContext` alias moves too)

- [ ] **Step 1: Create `internal/wire/doc.go`**

```go
// Package wire declares the JSON wire shapes shared by the atropos SDK
// hosts (the in-process Serve embed and the atroposd sidecar) and the
// manteion control plane. Every type here is decoded from / encoded to one
// of the SDK's HTTP endpoints or embedded in the manteion sync protocol.
// The root atropos package re-exports each type under the same name as a
// type alias, so the shapes manteion imports are unaffected by their move.
//
// Field sets and json tags are pinned by the cross-repo wire tests
// (wire_fidelity_test.go in the root package, wire_compat_test.go in
// internal/instrument) and must not drift.
package wire
```

- [ ] **Step 2: Move the fault shapes to `internal/wire/fault.go`**

Cut `FaultRequest`, its `effectiveCategory` method, and `FaultStatus` from `admin.go` (lines 147–183 at `de80d5e`) and `NetworkEnvelope` from `compiled_rule.go` (lines 57–79). The method is exported because `internal/instrument` calls it:

```go
package wire

import "encoding/json"

// FaultRequest is the platform's single fault wire shape, shared by the
// admin endpoint (POST /admin/fault), manteion's active_faults set, and —
// via the CompiledFault alias — the fault carried inside a CompiledRule.
//
// The category determines which sibling field is populated:
//   - "inline"   → Params holds the type-specific params; no envelope.
//   - "network"  → Network envelope holds host/target/direction/scope;
//     Params holds the toxic-specific params.
//   - "resource" → Params holds the type-specific params; no envelope.
//
// Network is forbidden for non-network categories and required for network.
// Params shapes are the exported faultparams.* structs (one per
// (category, fault_type) pair); absent params decode as all-defaults.
type FaultRequest struct {
	ID         string `json:"id,omitempty"` // slot identity (service+category:type); unused on compiled rules
	Category   string `json:"category"`     // "inline" (default), "network", "resource"
	FaultType  string `json:"fault_type"`   // fault type within category
	DurationMs int64  `json:"duration_ms,omitempty"`
	RampUpMs   int64  `json:"ramp_up_ms,omitempty"`
	RampDownMs int64  `json:"ramp_down_ms,omitempty"`

	Network *NetworkEnvelope `json:"network,omitempty"`
	Params  json.RawMessage  `json:"params,omitempty"`
}

// EffectiveCategory returns Category, defaulting an empty value to "inline".
func (r *FaultRequest) EffectiveCategory() string {
	if r.Category == "" {
		return "inline"
	}
	return r.Category
}

// FaultStatus is the JSON response for GET /admin/fault.
type FaultStatus struct {
	Active bool            `json:"active"`
	Faults []*FaultRequest `json:"faults,omitempty"`
}

// NetworkEnvelope is the network-category-only envelope. It separates the
// "where does the toxic live and to which traffic does it apply" question
// from the toxic-specific params (delay, rate, etc.).
type NetworkEnvelope struct {
	// Host selects where the toxic runs.
	//   "proxy"  → TCP proxy sidecar (current behaviour; requires NetworkResolver)
	//   "inline" → in-process RoundTripper wrapping response.Body (not yet supported)
	Host string `json:"host"`

	// Target is the logical name of the upstream service, resolved by
	// NetworkResolver into a (listen, upstream) address pair.
	// Required when Host=="proxy"; ignored when Host=="inline".
	Target string `json:"target,omitempty"`

	// Direction selects which half of a proxied connection to apply the
	// toxic to. Required when Host=="proxy". For Host=="inline" only
	// "downstream" (response body) is meaningful.
	Direction string `json:"direction,omitempty"`

	// Scope is the fraction of connections (proxy) or requests (inline)
	// to which the toxic applies. Zero means 1.0 (all).
	Scope float64 `json:"scope,omitempty"`
}
```

Every remaining `effectiveCategory()` call in root (`admin.go`, `compiled_rule.go`, `register.go`) becomes `EffectiveCategory()`.

- [ ] **Step 3: Move the rule shapes to `internal/wire/rules.go`**

Cut `CompiledRule`, `CompiledCacheBox`, `CompiledFault`, `CompiledComposition`, `CompiledCompositionMember` from `compiled_rule.go` (lines 22–99 minus `NetworkEnvelope`), `RuleSync` from `sync.go`, and add the `StaticRule` alias:

```go
package wire

import "git.ucsc.edu/microfaults/atropos-go/internal/evaluator"

// CompiledRule is the wire format received from manteion's poll/register
// endpoints. Manteion resolves FaultSpec references and inlines config;
// the SDK decodes and constructs evaluator rules locally.
//
// Exactly one of Fault, Composition, or CacheBox must be set.
type CompiledRule struct {
	Name           string               `json:"name"`
	InjectionPoint string               `json:"injection_point,omitempty"`
	Labels         map[string]string    `json:"labels,omitempty"`
	Mode           string               `json:"mode"`
	Priority       int                  `json:"priority"`
	StartPolicy    string               `json:"start_policy,omitempty"`
	Fault          *CompiledFault       `json:"fault,omitempty"`
	Composition    *CompiledComposition `json:"composition,omitempty"`
	CacheBox       *CompiledCacheBox    `json:"cachebox,omitempty"`
}

// CompiledCacheBox is a resolved cache-box action for a rule. Context, when
// present, is authoritative end-to-end (design doc Q3): its own
// KeyStrategy/StrategyVersion/KeyHeaders take precedence over the sibling
// KeyStrategy field.
type CompiledCacheBox struct {
	Mode        string           `json:"mode"`         // "passthrough" | "replay" | "replay_with_delay"
	KeyStrategy string           `json:"key_strategy"` // "exact" | "exact_with_host" | "exact_with_body"
	Context     *CacheBoxContext `json:"context,omitempty"`
}

// CompiledFault is a resolved fault spec on the wire: the same struct as
// FaultRequest, with ID unused in the compiled-rule context.
type CompiledFault = FaultRequest

// CompiledComposition is a resolved FaultComposition tree with all specs
// inlined. Composition execution is not yet supported by the SDK evaluator.
type CompiledComposition struct {
	Name          string                      `json:"name"`
	ExecutionMode string                      `json:"execution_mode"`
	DurationMs    int64                       `json:"duration_ms,omitempty"`
	RampUpMs      int64                       `json:"ramp_up_ms,omitempty"`
	RampDownMs    int64                       `json:"ramp_down_ms,omitempty"`
	Members       []CompiledCompositionMember `json:"members"`
}

// CompiledCompositionMember is a resolved member — either a leaf fault or a
// nested composition.
type CompiledCompositionMember struct {
	Direction   string               `json:"direction,omitempty"`
	Fault       *CompiledFault       `json:"fault,omitempty"`
	Composition *CompiledComposition `json:"composition,omitempty"`
}

// RuleSync is the desired-state payload manteion delivers to SDKs — the body
// of a 200 poll response (GET /api/v1/sdk/rules) and, embedded in
// RegisterResponse, the intent piggybacked on registration.
//
// Rules and ActiveFaults are deliberately NOT omitempty: under Apply's
// reconciliation an empty array means "clear everything in this category".
// A null/absent rules field (nil after decode) means "no change"; [] is
// authoritative desired state.
type RuleSync struct {
	Version      uint64         `json:"version"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
}

// StaticRule is the JSON shape GET /admin/rules serves (one element per
// installed rule). Manteion's SDK client decodes into it.
type StaticRule = evaluator.StaticRule
```

- [ ] **Step 4: Move registration shapes to `internal/wire/register.go`**

Cut `RegisterRequest`, `RegisterResponse` from `register.go` (lines 15–37) and `Route` from `routes.go`:

```go
package wire

// RegisterRequest is the POST body for /api/v1/sdk/register.
type RegisterRequest struct {
	ID             string `json:"id"`
	Service        string `json:"service"`
	Version        string `json:"version,omitempty"`
	Address        string `json:"address"`
	PollIntervalMs int64  `json:"poll_interval_ms,omitempty"` // SDK's poll cadence; manteion uses it for liveness detection
	// Routes is the HTTP route inventory for manteion's workflow-builder
	// catalog. Optional — gRPC services register without routes.
	Routes []Route `json:"routes,omitempty"`
}

// RegisterResponse is the JSON manteion returns from /api/v1/sdk/register:
// a status string plus the same RuleSync desired-state payload the poll
// endpoint serves, so the SDK converges before its first poll.
type RegisterResponse struct {
	Status string `json:"status"`
	RuleSync
}

// Route is one HTTP route published to manteion at registration time.
// Path is the literal template the service serves (e.g. "/product/{id}").
// DependsOn names prerequisite routes as "METHOD /path" (same service) or
// "service METHOD /path" (fully qualified).
type Route struct {
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Description string   `json:"description,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
}
```

- [ ] **Step 5: Move cache-box shapes to `internal/wire/cachebox.go`**

Cut `DelayRequest` from `cachebox_admin.go` (lines 57–67) and the whole of `cachebox_fidelity.go` (the `CacheBoxContext` alias, `DrainReport`, `DrainReportResponse`, the seven `Preload*` types, the four `Fidelity*` types), plus the `CacheBoxWireEntry`/`CacheBoxStats` aliases from `types.go`. Keep every doc comment. The header of the new file:

```go
package wire

import (
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// CacheBoxContext (wire spec §W1) scopes a cache-box operation to one
// experiment phase and pins the key strategy. Declared in internal/cachebox
// so the evaluator and interceptor can reference it without a cycle.
type CacheBoxContext = cachebox.CacheBoxContext

// CacheBoxWireEntry is the JSON-serializable transfer format for cache
// entries (ingest and preload).
type CacheBoxWireEntry = cachebox.WireEntry

// CacheBoxStats is the GET /admin/cachebox response.
type CacheBoxStats = cachebox.Stats

// DelayRequest is the JSON body for POST /admin/cachebox/delay -- the freeze
// command manteion sends at freezeServices time.
type DelayRequest struct {
	Mu      float64          `json:"mu"`
	Sigma   float64          `json:"sigma"`
	Seed    uint64           `json:"seed"`
	Context *CacheBoxContext `json:"context,omitempty"`
}
```

followed verbatim by the `DrainReport` … `FidelitySnapshot` declarations from `cachebox_fidelity.go`.

- [ ] **Step 6: Move `HealthStatus`, `ErrorResponse`, `NetworkResolver`**

`internal/wire/health.go`:

```go
package wire

import "time"

// HealthStatus reports the SDK's readiness for service health checks.
type HealthStatus struct {
	Status               string    `json:"status"`
	RuleVersion          uint64    `json:"rule_version"`
	RuleCount            int       `json:"rule_count"`
	ManteionURL          string    `json:"manteion_url,omitempty"`
	LastSuccessfulPollAt time.Time `json:"last_successful_poll_at,omitempty"`
	// StaleFor is the human-readable duration since LastSuccessfulPollAt.
	StaleFor string `json:"stale_for,omitempty"`
}
```

`internal/wire/errors.go`:

```go
package wire

// ErrorResponse is the JSON envelope returned by admin handler errors.
type ErrorResponse struct {
	Error string `json:"error" example:"invalid json"`
}
```

`internal/wire/resolver.go`:

```go
package wire

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies. Supplied via atropos.Config.NetworkResolver.
type NetworkResolver func(target string) (listen, upstream string, err error)
```

- [ ] **Step 7: Rewrite root `types.go` as the complete alias file**

```go
// types.go re-exports, as transparent type aliases, every wire shape the
// root package has always exported. Declarations live in internal/wire
// (and internal/cachebox, internal/evaluator, internal/interceptor); the
// names, fields and json tags here are pinned by the cross-repo wire tests
// and must not drift: manteion imports them as atroposdk.X.
package atropos

import (
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// Interceptor ties the evaluator, fault execution, and OTel together. It is
// exported only as the currency between DefaultInterceptor and the grpc
// subpackage's interceptor constructors.
type Interceptor = interceptor.Interceptor

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies. Supplied via Config.NetworkResolver.
type NetworkResolver = wire.NetworkResolver

// Faults and rules.
type (
	FaultRequest              = wire.FaultRequest
	FaultStatus               = wire.FaultStatus
	NetworkEnvelope           = wire.NetworkEnvelope
	CompiledRule              = wire.CompiledRule
	CompiledCacheBox          = wire.CompiledCacheBox
	CompiledFault             = wire.CompiledFault
	CompiledComposition       = wire.CompiledComposition
	CompiledCompositionMember = wire.CompiledCompositionMember
	RuleSync                  = wire.RuleSync
	StaticRule                = wire.StaticRule
)

// Registration.
type (
	RegisterRequest  = wire.RegisterRequest
	RegisterResponse = wire.RegisterResponse
	Route            = wire.Route
)

// Cache-box control, preload (W4/W5), drain (W3), fidelity (W6).
type (
	CacheBoxContext       = wire.CacheBoxContext
	CacheBoxWireEntry     = wire.CacheBoxWireEntry
	CacheBoxStats         = wire.CacheBoxStats
	DelayRequest          = wire.DelayRequest
	DrainReport           = wire.DrainReport
	DrainReportResponse   = wire.DrainReportResponse
	PreloadBeginRequest   = wire.PreloadBeginRequest
	PreloadBeginResponse  = wire.PreloadBeginResponse
	PreloadChunkRequest   = wire.PreloadChunkRequest
	PreloadChunkResponse  = wire.PreloadChunkResponse
	PreloadCommitRequest  = wire.PreloadCommitRequest
	PreloadCommitResponse = wire.PreloadCommitResponse
	PreloadAbortRequest   = wire.PreloadAbortRequest
	FidelityMissReasons   = wire.FidelityMissReasons
	FidelityReplayAge     = wire.FidelityReplayAge
	FidelityPreloadState  = wire.FidelityPreloadState
	FidelitySnapshot      = wire.FidelitySnapshot
)

// Health and errors.
type (
	HealthStatus  = wire.HealthStatus
	ErrorResponse = wire.ErrorResponse
)
```

Remove the `Interceptor` alias from `atropos.go` (it now lives here). `errors.go` keeps only `jsonError` for now (Task 3 moves it). Delete `sync.go`, `routes.go`, `cachebox_fidelity.go`.

- [ ] **Step 8: Build, test, and pin exported names**

Run: `go build ./... && go vet ./... && go test -count=1 ./... && go doc -all . | grep -E '^(func|type) [A-Z]' | wc -l`
Expected: all packages `ok`; the exported-name count is **41** (the 40 names at `de80d5e`, unchanged, plus none removed; `type CompiledFault = FaultRequest` still prints). Any name missing from `go doc` output compared with the list in the plan header's invariant is a plan failure.

- [ ] **Step 9: Commit**

```bash
git add internal/wire types.go admin.go compiled_rule.go register.go cachebox_admin.go health.go errors.go sync.go routes.go cachebox_fidelity.go atropos.go
git commit -m "refactor(m0): move wire shapes into internal/wire; root keeps type aliases"
```

---

### Task 3: Move the instrument logic into `internal/instrument`

**Files (moved with `git mv`, then `package atropos` → `package instrument`):**
- `admin.go` → `internal/instrument/admin_fault.go`
- `rules_admin.go` → `internal/instrument/admin_rules.go`
- `cachebox_admin.go` → `internal/instrument/admin_cachebox.go`
- `cachebox_preload.go` → `internal/instrument/preload.go`
- `cachebox_fidelity_handler.go` → `internal/instrument/fidelity_handler.go`
- `compiled_rule.go` → `internal/instrument/decode.go`
- `register.go` → `internal/instrument/register.go`
- `manteion.go`, `manteion_client.go` → same names under `internal/instrument/`
- `cache_push.go`, `cache_drain.go`, `telemetry.go`, `errors.go` → same names under `internal/instrument/`
- `health.go` → split: handler + `healthFrom` → `internal/instrument/health.go`; root `health.go` keeps `Health()`
- Tests: `admin_test.go`, `rules_admin_test.go`, `rules_admin_internal_test.go`, `cachebox_admin_test.go`, `cachebox_preload_test.go`, `cachebox_fidelity_handler_test.go`, `compiled_rule_test.go`, `register_test.go`, `manteion_test.go`, `routes_test.go`, `cache_push_test.go`, `cache_drain_test.go`, `telemetry_test.go`, `wire_compat_test.go` → `internal/instrument/` (same basenames; `compiled_rule_test.go` → `decode_test.go`, `admin_test.go` → `admin_fault_test.go`, `rules_admin_*` → `admin_rules_*`, `cachebox_admin_test.go` → `admin_cachebox_test.go`, `cachebox_preload_test.go` → `preload_test.go`, `cachebox_fidelity_handler_test.go` → `fidelity_handler_test.go`)
- Modify: `serve.go`, `atropos.go`, `health.go` (root)

- [ ] **Step 1: `git mv` every file listed above and rewrite the package clause**

```bash
mkdir -p internal/instrument
git mv admin.go internal/instrument/admin_fault.go
git mv admin_test.go internal/instrument/admin_fault_test.go
git mv rules_admin.go internal/instrument/admin_rules.go
git mv rules_admin_test.go internal/instrument/admin_rules_test.go
git mv rules_admin_internal_test.go internal/instrument/admin_rules_internal_test.go
git mv cachebox_admin.go internal/instrument/admin_cachebox.go
git mv cachebox_admin_test.go internal/instrument/admin_cachebox_test.go
git mv cachebox_preload.go internal/instrument/preload.go
git mv cachebox_preload_test.go internal/instrument/preload_test.go
git mv cachebox_fidelity_handler.go internal/instrument/fidelity_handler.go
git mv cachebox_fidelity_handler_test.go internal/instrument/fidelity_handler_test.go
git mv compiled_rule.go internal/instrument/decode.go
git mv compiled_rule_test.go internal/instrument/decode_test.go
git mv register.go internal/instrument/register.go
git mv register_test.go internal/instrument/register_test.go
git mv manteion.go internal/instrument/manteion.go
git mv manteion_client.go internal/instrument/manteion_client.go
git mv manteion_test.go internal/instrument/manteion_test.go
git mv routes_test.go internal/instrument/routes_test.go
git mv cache_push.go internal/instrument/cache_push.go
git mv cache_push_test.go internal/instrument/cache_push_test.go
git mv cache_drain.go internal/instrument/cache_drain.go
git mv cache_drain_test.go internal/instrument/cache_drain_test.go
git mv telemetry.go internal/instrument/telemetry.go
git mv telemetry_test.go internal/instrument/telemetry_test.go
git mv errors.go internal/instrument/errors.go
git mv wire_compat_test.go internal/instrument/wire_compat_test.go
sed -i '' 's/^package atropos$/package instrument/' internal/instrument/*.go
```

- [ ] **Step 2: Reference wire types by package inside `internal/instrument`**

Every moved file that names a wire type unqualified (`FaultRequest`, `CompiledRule`, `RuleSync`, `RegisterRequest`, `RegisterResponse`, `Route`, `DelayRequest`, `NetworkEnvelope`, `NetworkResolver`, `Preload*`, `Fidelity*`, `DrainReport`, `DrainReportResponse`, `StaticRule`, `HealthStatus`, `FaultStatus`, `CompiledFault`, `CompiledCacheBox`, `CacheBoxContext`) imports `"git.ucsc.edu/microfaults/atropos-go/internal/wire"` and qualifies them as `wire.X`. Do it with one sed over the package, then fix the few identifiers the sed over-qualifies (there are none in the moved files that collide; check with `go build`):

```bash
cd internal/instrument
for T in FaultRequest FaultStatus CompiledRule CompiledCacheBox CompiledFault CompiledComposition CompiledCompositionMember RuleSync StaticRule RegisterRequest RegisterResponse Route CacheBoxContext DelayRequest DrainReportResponse DrainReport PreloadBeginRequest PreloadBeginResponse PreloadChunkRequest PreloadChunkResponse PreloadCommitRequest PreloadCommitResponse PreloadAbortRequest FidelityMissReasons FidelityReplayAge FidelityPreloadState FidelitySnapshot HealthStatus NetworkEnvelope NetworkResolver ErrorResponse; do
  sed -i '' -E "s/([^A-Za-z0-9_.])${T}([^A-Za-z0-9_])/\1wire.${T}\2/g" *.go
done
goimports -w . 2>/dev/null || gofmt -w .
```

(If `goimports` is not installed, add the `wire` import by hand to each file the build reports.)

- [ ] **Step 3: Export the identifiers the root host (and M1's daemon) call, and thread the fault registry**

Rename, package-wide (`sed -i '' 's/\bold\b/New/g' internal/instrument/*.go`), exactly these:

| old | new | file |
|---|---|---|
| `demoEvaluator` | `DemoEvaluator` | admin_fault.go |
| `faultAdminHandler` | `FaultAdminHandler` | admin_fault.go |
| `buildFault` | `BuildFault` | admin_fault.go |
| `rulesAdminHandler` | `RulesAdminHandler` | admin_rules.go |
| `cacheBoxAdminHandler` | `CacheBoxAdminHandler` | admin_cachebox.go |
| `cacheBoxPreloadHandler` | `CacheBoxPreloadHandler` | preload.go |
| `cacheBoxFidelityHandler` | `CacheBoxFidelityHandler` | fidelity_handler.go |
| `decodeCompiledRules` | `DecodeCompiledRules` | decode.go |
| `decodeOption` | `DecodeOption` | decode.go |
| `withNetworkResolver` | `WithNetworkResolver` | decode.go |
| `registerWith` | `RegisterWith` | register.go |
| `applyTargets` | `ApplyTargets` | register.go |
| `apply(` | `Apply(` | register.go (function only; keep local variable names) |
| `applyActiveFault` | `ApplyActiveFault` | register.go |
| `applyFreezeCfg` | `ApplyFreezeCfg` | register.go |
| `startFaultWatchdog` | `StartFaultWatchdog` | register.go |
| `connectManteion` | `ConnectManteion` | manteion.go |
| `manteionConfig` | `ManteionConfig` | manteion.go |
| `defaultManteionConfig` | `DefaultManteionConfig` | manteion.go |
| `manteionClient` | `ManteionClient` | manteion_client.go |
| `healthHandler` | `HealthHandler` | health.go |
| `healthFrom` | `HealthFrom` | health.go |
| `newCachePushClient` | `NewCachePushClient` | cache_push.go |
| `cachePushConfig` | `CachePushConfig` | cache_push.go |
| `cachePushClient` | `CachePushClient` | cache_push.go |
| `cachePushStats` | `CachePushStats` | cache_push.go |
| `newCacheDrainTracker` | `NewCacheDrainTracker` | cache_drain.go |
| `cacheDrainTracker` | `CacheDrainTracker` | cache_drain.go |
| `initTelemetry` | `InitTelemetry` | telemetry.go |
| `telemetryConfig` | `TelemetryConfig` | telemetry.go |

`ManteionConfig` fields become exported (`ServiceName`, `ServiceVersion`, `Address`, `URL`, `InstanceID`, `Routes`, `InitTimeout`, `PollInterval`, `Offline`, `Targets`, `HTTPClient`, `Logger`, `AuthFn`); `TelemetryConfig` fields become exported (`ServiceName`, `ServiceVersion`, `Environment`, `Endpoint`, `UseHTTP`, `Secure`, `Sampler`, `TracerProvider`). Update every use in the moved files and tests.

Registry threading (this is the one semantic seam; the behavior is identical, the registry is just no longer a package global):

```go
// register.go — ApplyTargets gains the registry that owns background faults.
type ApplyTargets struct {
	Evaluator       *evaluator.StaticEvaluator
	DemoEval        *DemoEvaluator
	CacheBox        *cachebox.CacheBox
	NetworkResolver wire.NetworkResolver
	CacheDrain      *CacheDrainTracker
	// Registry holds every running background fault; rule/slot removal stops
	// faults through it. Required whenever Evaluator or DemoEval is set.
	Registry *interceptor.FaultRegistry
}

// stopFaults cancels the running background faults started under keys.
// Nil-safe on the registry.
func stopFaults(reg *interceptor.FaultRegistry, keys ...string) {
	for _, k := range keys {
		reg.Stop(k)
	}
}
```

Replace every `stopBackgroundFaults(x...)` in the moved files with `stopFaults(targets.Registry, x...)` (register.go `Apply`), `stopFaults(reg, x...)` in `StartFaultWatchdog(ctx, eval, reg, pollInterval, logger)` (new `reg *interceptor.FaultRegistry` parameter, passed from `ConnectManteion` as `cfg.Targets.Registry`), `applyRuleSet(reg, eval, rules)` in admin_rules.go (new first parameter; `RulesAdminHandler(eval *evaluator.StaticEvaluator, reg *interceptor.FaultRegistry, opts ...DecodeOption)`), and `FaultAdminHandler(eval *DemoEvaluator, reg *interceptor.FaultRegistry, resolve wire.NetworkResolver)` in admin_fault.go.

Delete `setGlobalClient`/`globalClient` from `internal/instrument/health.go` and the `setGlobalClient(c)` call in `ConnectManteion`; `HealthHandler(c *ManteionClient)` takes the client explicitly (nil-safe: `HealthFrom(nil)` already returns `offline`).

- [ ] **Step 4: Root `health.go` keeps the public `Health()`**

```go
package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
)

// globalClient is set by Serve so Health() works without threading the
// client everywhere. Cleared on shutdown.
var globalClient atomic.Pointer[instrument.ManteionClient]

// Health returns the current SDK health status. With no control plane
// configured (offline mode), Status is "offline".
func Health() HealthStatus {
	return instrument.HealthFrom(globalClient.Load())
}
```

- [ ] **Step 5: Root `atropos.go` drops `stopBackgroundFaults` and `configure` keeps the registry**

```go
package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
)

var (
	// defaultInterceptor is the process-wide policy dispatcher, resolved per
	// request/round-trip so an EgressTransport built before Serve runs still
	// routes through the evaluator Serve installs.
	defaultInterceptor atomic.Pointer[interceptor.Interceptor]

	// defaultRegistry holds every running background fault. Created ONCE and
	// handed to the core Serve builds: the registry is process-lifetime and
	// never swapped, so reconfiguring cannot orphan running faults (A1).
	defaultRegistry *interceptor.FaultRegistry
)

func init() {
	defaultRegistry = interceptor.NewFaultRegistry()
	defaultInterceptor.Store(interceptor.New(nil, trace.NewOTelTracer(),
		interceptor.WithRegistry(defaultRegistry),
		interceptor.WithInjectionHook(metrics.RecordFaultInjection)))
}

func currentInterceptor() *interceptor.Interceptor { return defaultInterceptor.Load() }

// DefaultInterceptor returns the package-level interceptor installed by
// Serve (or the tracing-only baseline before Serve runs), for the grpc
// subpackage's constructors.
func DefaultInterceptor() *Interceptor { return currentInterceptor() }

// configure swaps the package-level interceptor to one built around eval
// and cb over the process-lifetime registry. Serve installs the core's
// interceptor directly; this remains for tests that pin a private evaluator.
func configure(eval evaluator.Evaluator, cb *cachebox.CacheBox) {
	opts := []interceptor.Option{
		interceptor.WithInjectionHook(metrics.RecordFaultInjection),
		interceptor.WithRegistry(defaultRegistry),
	}
	if cb != nil {
		opts = append(opts, interceptor.WithCacheBox(cb))
	}
	defaultInterceptor.Store(interceptor.New(eval, trace.NewOTelTracer(), opts...))
}
```

- [ ] **Step 6: Root `serve.go` calls the moved code directly (Core comes in Task 4)**

Replace the body of `Serve` so every former unexported call is the instrument equivalent: `initTelemetry` → `instrument.InitTelemetry(ctx, instrument.TelemetryConfig{ServiceName: cfg.Service, ServiceVersion: version, Environment: cfg.Environment, TracerProvider: cfg.TracerProvider})`; `newCachePushClient(cachePushConfig{...})` → `instrument.NewCachePushClient(instrument.CachePushConfig{...})`; `&demoEvaluator{}` → `&instrument.DemoEvaluator{}`; `configure(evaluator.NewMultiEvaluator(host, demo), cb)` unchanged; `applyTargets{...}` → `instrument.ApplyTargets{..., Registry: defaultRegistry}`; `newCacheDrainTracker` → `instrument.NewCacheDrainTracker`; `defaultManteionConfig` → `instrument.DefaultManteionConfig` with exported field names; `connectManteion` → `instrument.ConnectManteion`; after a successful connect, `globalClient.Store(mc)`; in `shutdown`, `globalClient.CompareAndSwap(mc, nil)` stays. In `controlMux`, replace the handler constructors with the exported ones, passing `defaultRegistry` where the signature now requires it, `metrics.Handler()` for `/metrics`, and `instrument.HealthHandler(mc)` for `/atropos/health` (pass `mc` into `controlMux`).

- [ ] **Step 7: Fix the moved tests that used root globals**

`admin_fault_test.go` `TestAdminFault_ComposesWithHostEvaluator_RegistryUntouched`: replace `configure(...)`/`currentInterceptor()` with a local interceptor and `defaultRegistry` with a local registry:

```go
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	ic := interceptor.New(evaluator.NewMultiEvaluator(host, demo), nil, interceptor.WithRegistry(reg))

	h, deduped, err := reg.StartOrJoin("host-bg", evaluator.DeduplicateByRule, seedLongFault)
	...
	handler := FaultAdminHandler(demo, reg, nil)
	...
	d := ic.Evaluate(context.Background(), evaluator.Request{Point: evaluator.Egress})
	...
	// (c) Arm + clear left the seeded fault alone: still running, still
	// stoppable through the same registry.
	select {
	case <-h.Done():
		t.Fatal("admin arm/clear cancelled a host background fault")
	case <-time.After(100 * time.Millisecond):
	}
	reg.Stop("host-bg")
```

(the `regBefore` pointer-identity assertion is dropped: with the registry passed explicitly there is no longer any code path that could swap it). The other `admin_fault_test.go` cases construct `FaultAdminHandler(eval, nil, resolve)` with a nil registry where they never arm background faults, and a fresh `interceptor.NewFaultRegistry()` where they do (`TestFaultAdmin_PostCPUStress`, `TestFaultAdmin_PostNetworkLatency`).

`admin_rules_internal_test.go`: `defaultRegistry` → a local `reg := interceptor.NewFaultRegistry()`; `applyRuleSet(eval, ...)` → `applyRuleSet(reg, eval, ...)`.

`manteion_test.go` `newTestTargets()` returns `ApplyTargets{Evaluator: newTestEvaluator(), Registry: interceptor.NewFaultRegistry()}`. Every `applyTargets{...}` literal in `register_test.go`, `cache_drain_test.go`, `wire_compat_test.go` gains `Registry: interceptor.NewFaultRegistry()` when it sets `Evaluator` or `DemoEval`.

- [ ] **Step 8: Adapt the root tests that referenced moved symbols**

`integration_test.go`: `initTelemetry(context.Background(), telemetryConfig{tracerProvider: tp})` → `instrument.InitTelemetry(context.Background(), instrument.TelemetryConfig{TracerProvider: tp})`. Nothing else in root tests referenced moved code (`serve_test.go`, `middleware_test.go`, `wire_fidelity_test.go`, `metrics_test.go` use exported names only).

- [ ] **Step 9: Build and run the whole suite, including race**

Run: `gofmt -l . ; go build ./... && go vet ./... && go test -count=1 ./... && go test -race -count=1 . ./internal/instrument/`
Expected: `gofmt -l` prints nothing; every package `ok`.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "refactor(m0): move the instrument core into internal/instrument; Serve becomes the in-process host"
```

---

### Task 4: `Core`, `ControlSurface`, `ResolveInstanceID`

**Files:**
- Create: `internal/instrument/core.go`, `internal/instrument/identity.go`, `internal/instrument/control_surface.go`
- Test: `internal/instrument/core_test.go`, `internal/instrument/control_surface_test.go`
- Modify: `serve.go`

- [ ] **Step 1: Write the failing core test**

```go
package instrument

import (
	"context"
	"log/slog"
	"testing"

	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
)

func TestNewCore_OfflineHasNoPushAndSharesFidelity(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler)})
	t.Cleanup(c.Stop)

	if c.Push != nil {
		t.Fatal("offline core must not build a push client")
	}
	if c.CacheBox.Fidelity() != c.Fidelity {
		t.Fatal("cache-box must count into the core's fidelity registry")
	}
	if c.Interceptor.CacheBox() != c.CacheBox {
		t.Fatal("interceptor must dispatch cache-box through the core's cache-box")
	}
	if c.Registry == nil {
		t.Fatal("core must own a fault registry when none is supplied")
	}
}

func TestNewCore_ConnectedBuildsPushAndDrain(t *testing.T) {
	reg := interceptor.NewFaultRegistry()
	t.Cleanup(reg.Close)
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1", ManteionURL: "http://manteion:8080", Registry: reg})
	t.Cleanup(c.Stop)

	if c.Push == nil {
		t.Fatal("connected core must build a push client")
	}
	if c.Registry != reg {
		t.Fatal("core must adopt the supplied registry, not replace it")
	}
	tg := c.Targets(nil)
	if tg.Evaluator != c.Host || tg.DemoEval != c.Demo || tg.CacheBox != c.CacheBox || tg.Registry != reg || tg.CacheDrain == nil {
		t.Fatalf("Targets() must expose the core's components: %+v", tg)
	}
}

func TestCore_HostRulesWinOverAdminSlot(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1"})
	t.Cleanup(c.Stop)
	c.Host.SetRules([]evaluator.StaticRule{{
		Name: "host", Point: evaluator.Egress,
		Decision: evaluator.Decision{Name: "host", CacheBox: evaluator.CacheBoxReplay, Reason: "compiled"},
	}})
	c.Demo.Set(&evaluator.Decision{Name: "admin", Reason: "admin"}, &wire.FaultRequest{ID: "admin"})

	if d := c.Interceptor.Evaluate(context.Background(), evaluator.Request{Point: evaluator.Egress}); d == nil || d.Name != "host" {
		t.Fatalf("egress decision = %+v, want host rule", d)
	}
	if d := c.Interceptor.Evaluate(context.Background(), evaluator.Request{Point: evaluator.Ingress}); d == nil || d.Reason != "admin" {
		t.Fatalf("ingress decision = %+v, want admin slot", d)
	}
}
```

(add `"git.ucsc.edu/microfaults/atropos-go/internal/wire"` to the imports.)

- [ ] **Step 2: Run it to verify it fails**

Run: `go test -count=1 ./internal/instrument/ -run 'TestNewCore|TestCore_' -v`
Expected: FAIL to compile with `undefined: NewCore`.

- [ ] **Step 3: Write `core.go`**

```go
package instrument

import (
	"log/slog"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/trace"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// CoreConfig is the input to NewCore.
type CoreConfig struct {
	// Service and Instance are the identity every component stamps: push
	// envelopes, drain reports, the fidelity endpoint.
	Service  string
	Instance string
	// ManteionURL, when non-empty, builds the cache push client and the
	// drain tracker. Empty = offline: recording still counts into the
	// fidelity registry, nothing is pushed.
	ManteionURL string
	// Registry, if set, is adopted as the process-lifetime fault registry;
	// nil builds a fresh one. The in-process host passes its package-level
	// registry so reconfiguration never orphans running faults (A1).
	Registry *interceptor.FaultRegistry
	// InjectionHook defaults to metrics.RecordFaultInjection.
	InjectionHook interceptor.InjectionHook
	Logger        *slog.Logger
}

// Core is the instrument: one shared fidelity registry, the cache-box and its
// push client, the host rule evaluator composed before the admin fault slot
// (host rules always win), the fault registry, the interceptor middleware
// layers dispatch through, and the drain tracker. Every host of the SDK (the
// in-process Serve embed, the atroposd sidecar) builds exactly one Core.
type Core struct {
	Service  string
	Instance string

	Fidelity    *cachebox.FidelityRegistry
	CacheBox    *cachebox.CacheBox
	Push        *CachePushClient // nil when offline
	Host        *evaluator.StaticEvaluator
	Demo        *DemoEvaluator
	Registry    *interceptor.FaultRegistry
	Interceptor *interceptor.Interceptor
	Drain       *CacheDrainTracker // nil when offline
	Logger      *slog.Logger
}

// NewCore wires the instrument. The construction order is what the hand-wired
// embeds used to get wrong: the fidelity registry exists first and is handed
// to both the push client and the cache-box, so record-side and push-side
// counts land in the one registry the drain report and fidelity endpoint
// snapshot.
func NewCore(cfg CoreConfig) *Core {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	hook := cfg.InjectionHook
	if hook == nil {
		hook = metrics.RecordFaultInjection
	}
	reg := cfg.Registry
	if reg == nil {
		reg = interceptor.NewFaultRegistry()
	}

	fidelity := cachebox.NewFidelityRegistry()
	var push *CachePushClient
	cbCfg := cachebox.Config{Fidelity: fidelity}
	if cfg.ManteionURL != "" {
		push = NewCachePushClient(CachePushConfig{
			BaseURL:  cfg.ManteionURL,
			Service:  cfg.Service,
			Instance: cfg.Instance,
			Logger:   logger,
			Fidelity: fidelity,
		})
		cbCfg.Push = push.PushFunc()
	}
	cb := cachebox.New(cbCfg)

	host := evaluator.NewStaticEvaluator()
	demo := &DemoEvaluator{}
	ic := interceptor.New(evaluator.NewMultiEvaluator(host, demo), trace.NewOTelTracer(),
		interceptor.WithInjectionHook(hook),
		interceptor.WithRegistry(reg),
		interceptor.WithCacheBox(cb),
	)

	c := &Core{
		Service: cfg.Service, Instance: cfg.Instance,
		Fidelity: fidelity, CacheBox: cb, Push: push,
		Host: host, Demo: demo, Registry: reg, Interceptor: ic, Logger: logger,
	}
	if push != nil {
		c.Drain = NewCacheDrainTracker(cb, push, logger)
	}
	return c
}

// Targets is the ApplyTargets the control plane mutates for this core.
func (c *Core) Targets(resolver wire.NetworkResolver) ApplyTargets {
	return ApplyTargets{
		Evaluator:       c.Host,
		DemoEval:        c.Demo,
		CacheBox:        c.CacheBox,
		NetworkResolver: resolver,
		CacheDrain:      c.Drain,
		Registry:        c.Registry,
	}
}

// StopFaults cancels the running background faults under keys.
func (c *Core) StopFaults(keys ...string) { stopFaults(c.Registry, keys...) }

// Stop drains the recorder into the push client and flushes the push
// client's final batch. Order matters; idempotent.
func (c *Core) Stop() {
	c.CacheBox.Stop()
	if c.Push != nil {
		c.Push.Stop()
	}
}
```

- [ ] **Step 4: Write `identity.go`**

```go
package instrument

import "os"

// ResolveInstanceID picks the identity manteion correlates an SDK instance
// under. It MUST be identical across the register call, the cache-push
// envelopes/drain reports, and the fidelity endpoint, so hosts resolve it
// exactly once and thread it everywhere. Precedence: explicit >
// MANTEION_INSTANCE_ID > hostname (the pod name in k8s) > service name.
func ResolveInstanceID(explicit, service string) string {
	if explicit != "" {
		return explicit
	}
	if id := os.Getenv("MANTEION_INSTANCE_ID"); id != "" {
		return id
	}
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return service
}
```

- [ ] **Step 5: Write the failing control-surface test**

```go
package instrument

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlSurface_MountsEveryPathManteionAddresses(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1"})
	t.Cleanup(c.Stop)
	mux := ControlSurface(ControlConfig{Core: c})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	cases := []struct {
		method, path, body string
		want               int
	}{
		{"GET", "/metrics", "", 200},
		{"GET", "/atropos/health", "", 200},
		{"GET", "/admin/fault", "", 200},
		{"DELETE", "/admin/fault/x", "", 200},
		{"GET", "/admin/rules", "", 200},
		{"POST", "/admin/rules", "[]", 204},
		{"GET", "/admin/cachebox", "", 200},
		{"POST", "/admin/cachebox/delay", `{"mu":1,"sigma":0.1}`, 204},
		{"POST", "/cachebox/preload/abort", `{"experiment_id":"e","phase_id":"p"}`, 200},
		{"GET", "/cachebox/fidelity?experiment_id=e&phase_id=p", "", 200},
		{"GET", "/anything-else", "", 404},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
}
```

- [ ] **Step 6: Run it to verify it fails**

Run: `go test -count=1 ./internal/instrument/ -run TestControlSurface -v`
Expected: FAIL to compile with `undefined: ControlSurface`.

- [ ] **Step 7: Write `control_surface.go`**

```go
package instrument

import (
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// ControlConfig configures ControlSurface.
type ControlConfig struct {
	Core *Core
	// Client is the connected control-plane client, or nil in offline mode
	// (the health endpoint then reports "offline").
	Client *ManteionClient
	// NetworkResolver is forwarded to the rule decoders behind /admin/rules
	// and /admin/fault; nil rejects network-category faults.
	NetworkResolver wire.NetworkResolver
}

// ControlSurface mounts the runtime-control and observability endpoints at
// the exact paths manteion addresses:
//
//	GET  /metrics                    Prometheus metrics
//	     /atropos/health             SDK health/connectivity
//	     /admin/fault[/{id}]         arm/inspect/clear admin faults
//	     /admin/rules                inspect/replace the compiled rule set
//	     /admin/cachebox[/delay]     cache-box stats / freeze delay / clear
//	POST /cachebox/preload/*         staged replay-set preload (W4)
//	GET  /cachebox/fidelity          per-phase fidelity counters (W6)
//
// The returned mux has no "/" handler: hosts that serve business traffic
// beside the control surface add their own, wrapped in the fault/OTel
// middleware, and the control paths stay outside it.
func ControlSurface(cfg ControlConfig) *http.ServeMux {
	c := cfg.Core
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", metrics.Handler())
	mux.Handle("/atropos/health", HealthHandler(cfg.Client))

	// Bare path and subtree both: the subtree pattern alone would
	// 301-redirect the bare path and drop freeze/thaw verbs.
	faultAdmin := FaultAdminHandler(c.Demo, c.Registry, cfg.NetworkResolver)
	mux.Handle("/admin/fault", faultAdmin)
	mux.Handle("/admin/fault/", faultAdmin)

	var decodeOpts []DecodeOption
	if cfg.NetworkResolver != nil {
		decodeOpts = append(decodeOpts, WithNetworkResolver(cfg.NetworkResolver))
	}
	mux.Handle("/admin/rules", RulesAdminHandler(c.Host, c.Registry, decodeOpts...))

	cbAdmin := CacheBoxAdminHandler(c.CacheBox)
	mux.Handle("/admin/cachebox", cbAdmin)
	mux.Handle("/admin/cachebox/", cbAdmin)
	mux.Handle("/cachebox/preload/", CacheBoxPreloadHandler(c.CacheBox))
	mux.Handle("GET /cachebox/fidelity", CacheBoxFidelityHandler(c.CacheBox, c.Service, c.Instance))

	return mux
}
```

- [ ] **Step 8: Run both new tests**

Run: `go test -count=1 ./internal/instrument/ -run 'TestNewCore|TestCore_|TestControlSurface' -v`
Expected: PASS ×4.

- [ ] **Step 9: Rewrite root `serve.go` on top of Core**

```go
package atropos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// Config is the input to Serve — everything a service supplies to embed the
// instrument. (Field docs unchanged from de80d5e.)
type Config struct {
	Service         string
	Version         string
	Environment     string
	Routes          []Route
	Handler         http.Handler
	ManteionURL     string
	InstanceID      string
	NetworkResolver NetworkResolver
	TracerProvider  oteltrace.TracerProvider
	Logger          *slog.Logger
}

// ShutdownFunc tears the embed down: control-plane deregistration, recorder
// drain, final cache-push flush, and OTel span flush, in that order.
// Idempotent.
type ShutdownFunc func(context.Context) error

// Serve wires the complete instrument around the service's handler and
// returns the composed handler to listen on. (Doc comment unchanged.)
func Serve(ctx context.Context, cfg Config) (http.Handler, ShutdownFunc, error) {
	if cfg.Service == "" {
		return nil, nil, errors.New("atropos: Config.Service is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	version := cfg.Version
	if version == "" {
		version = os.Getenv("MANTEION_SERVICE_VERSION")
	}
	manteionURL := cfg.ManteionURL
	if manteionURL == "" {
		manteionURL = os.Getenv("MANTEION_URL")
	}
	instanceID := instrument.ResolveInstanceID(cfg.InstanceID, cfg.Service)

	otelShutdown, err := instrument.InitTelemetry(ctx, instrument.TelemetryConfig{
		ServiceName:    cfg.Service,
		ServiceVersion: version,
		Environment:    cfg.Environment,
		TracerProvider: cfg.TracerProvider,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("atropos: init telemetry: %w", err)
	}

	core := instrument.NewCore(instrument.CoreConfig{
		Service:     cfg.Service,
		Instance:    instanceID,
		ManteionURL: manteionURL,
		Registry:    defaultRegistry, // process-lifetime, never swapped (A1)
		Logger:      logger,
	})
	// Install the core's interceptor as the package-level dispatcher the
	// middleware, EgressTransport and the grpc subpackage resolve.
	defaultInterceptor.Store(core.Interceptor)

	mcfg := instrument.DefaultManteionConfig(cfg.Service)
	mcfg.URL = manteionURL
	mcfg.InstanceID = instanceID
	mcfg.ServiceVersion = version
	mcfg.Routes = cfg.Routes
	mcfg.Targets = core.Targets(cfg.NetworkResolver)
	mcfg.Logger = logger
	mc, err := instrument.ConnectManteion(ctx, mcfg)
	if err != nil {
		core.Stop()
		_ = otelShutdown(ctx)
		return nil, nil, fmt.Errorf("atropos: connect manteion: %w", err)
	}
	globalClient.Store(mc)

	mux := instrument.ControlSurface(instrument.ControlConfig{
		Core:            core,
		Client:          mc,
		NetworkResolver: cfg.NetworkResolver,
	})
	if cfg.Handler != nil {
		mux.Handle("/", ingressMiddleware(cfg.Handler, cfg.Service, currentInterceptor()))
	}

	var once sync.Once
	var shutdownErr error
	shutdown := func(sctx context.Context) error {
		once.Do(func() {
			_ = mc.Close(sctx) // nil-safe in offline mode
			core.Stop()
			globalClient.CompareAndSwap(mc, nil)
			shutdownErr = otelShutdown(sctx)
		})
		return shutdownErr
	}
	return mux, shutdown, nil
}
```

(Keep the full field doc comments from `de80d5e` on `Config`; the block above abbreviates them only for the plan.) Delete the old `resolveInstanceID` and `controlMux` from root.

- [ ] **Step 10: Run the whole suite**

Run: `gofmt -l . ; go build ./... && go vet ./... && go test -count=1 ./... && go test -race -count=1 . ./internal/instrument/`
Expected: no gofmt output; every package `ok`; `serve_test.go` passes unchanged (it exercises Serve end-to-end: offline surface, middleware bypass, record→replay fail-closed, preload commit→hit, one identity everywhere, unreachable manteion fatal).

- [ ] **Step 11: Commit**

```bash
git add -A
git commit -m "refactor(m0): Core + ControlSurface in internal/instrument; Serve is a thin host"
```

---

### Task 5: OpenAPI check, docs, and the M0 gate

**Files:**
- Modify: `docs/swagger.yaml`, `docs/swagger.json` (only if swag output changes), `README.md`, `AGENTS.md`

- [ ] **Step 1: Regenerate the OpenAPI spec and confirm it is byte-identical**

Run: `make openapi-check`
Expected: exit 0 (the annotations reference root aliases; swag v2 with `--parseInternal --parseDependency` resolves them to the same schemas). If the diff is non-empty and the change is only schema *names* (e.g. `wire.FaultRequest` instead of `atropos.FaultRequest`), that is a wire-visible change for manteion's generated clients: stop and report rather than commit the new spec.

- [ ] **Step 2: Document the layout**

In `README.md`, under the "Architecture" diagram, add one paragraph:

```markdown
The root package is the in-process host only. `internal/instrument` is the
instrument core every host builds (`Core`, `ControlSurface`, the manteion
client, the rule decoders, the admin handlers); `internal/wire` holds the JSON
wire shapes, re-exported here as type aliases; `internal/metrics` holds the
Prometheus series. `cmd/atroposd` (the out-of-process sidecar host, see
`docs/plans/2026-09-08-atropos-daemon-design.md`) builds the same core.
```

In `AGENTS.md` § "SDK Bootstrap", after "Registration, polling, and intent application are internal to `Serve`", add: "They live in `internal/instrument`; the wire types below are aliases of `internal/wire`."

- [ ] **Step 3: Run the full gate once more and commit**

Run: `go build ./... && go vet ./... && go test -count=1 ./... && make openapi-check`
Expected: all `ok`, spec unchanged.

```bash
git add README.md AGENTS.md
git commit -m "docs(m0): describe the internal/instrument + internal/wire layout"
```

---

## Self-review

**Spec coverage (design §5.11):** `internal/wire` with root aliases — Task 2; `internal/instrument` with `Identity`, `Core`, `ControlSurface`, `ControlPlane` (`ConnectManteion`/`ApplyTargets`/`Apply`) — Tasks 3–4; root `Serve` as thin adapter, `EgressTransport` and gRPC interceptors unchanged — Task 4; `cmd/atroposd` — M1 plan, not this one; "no behavior change; the existing test suite is the regression gate" — every task ends with the full suite, plus race on the two packages that own goroutines, plus the OpenAPI byte-identity check (Task 5).

**Placeholder scan:** none; moved code is specified by exact `git mv`/`sed` commands and identifier tables, new code is complete.

**Type consistency:** `ApplyTargets.Registry` (Task 3) is what `Core.Targets` (Task 4) fills; `FaultAdminHandler(demo, reg, resolve)`, `RulesAdminHandler(eval, reg, opts...)`, `StartFaultWatchdog(ctx, eval, reg, interval, logger)`, `HealthHandler(client)`, `ConnectManteion(ctx, ManteionConfig)` are used with those exact shapes in Tasks 3, 4 and the root `serve.go`.
