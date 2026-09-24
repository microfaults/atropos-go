# atroposd M1: Explicit-Mode Daemon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `cmd/atroposd`, an out-of-process host of the instrument core: explicit-mode egress listeners (one per dependency), an ingress listener, HTTP/1.1 only, with inline faults, the cache-box, network toxics as an inline transport with client-leg reproduction, eager arming of background faults, the control surface on its own port, and proxy-overhead metrics. Design: `docs/plans/2026-09-08-atropos-daemon-design.md` §5 (M1 row of §6).

**Architecture:** The daemon builds one `instrument.Core` (M0) and puts two `httputil.ReverseProxy` instances around it. The **egress listener** rebuilds the client-side request view (scheme, `URL.Host`, `Host`), extracts trace context and baggage from the incoming headers, and forwards through exactly the transport composition the in-process host uses (`otelhttp` client span → interceptor fault/cache-box dispatch → metrics), so keys, headers, fail-closed behaviour and metrics are byte-identical. The **ingress listener** wraps a reverse proxy to the app in exactly the in-process ingress composition (metrics → `otelhttp` server span → interceptor fault check) and injects the sidecar's span context into the hop to the app. Network faults with `NetworkEnvelope.Host = "inline"` become a **`ToxicTransport`** under the egress transport, keyed by an armed **policy table**; RST and blackhole are reproduced on the app-facing connection by hijacking it. Background faults are **armed eagerly** when rules apply.

**Tech Stack:** Go 1.25, `net/http/httputil`, `otelhttp`, `gopkg.in/yaml.v3` (already a dependency), stdlib `testing` with `httptest`.

**Behaviour changes to the in-process host (deliberate, each with a test):**
1. The inline `error` fault now short-circuits the request (ingress: writes the status + message; egress: returns a synthetic response). Today `inline.IsErrorResult` has no caller, so the documented fault is a no-op on the wire.
2. Nothing else. `Serve` keeps lazy arming and the `host=inline` rejection (the daemon opts into both new behaviours through `CoreConfig`).

---

## File structure

```
internal/instrument/middleware.go        IngressMiddleware, EgressTransport (moved from root; + error short-circuit)
internal/instrument/core.go              CoreConfig.EagerArm, CoreConfig.DecodeOptions, Core.Arm
internal/instrument/decode.go            DecodeOption exported, WithInlineToxicHost, decodeConfig.inlineToxic
internal/instrument/register.go          applyTargets.DecodeOpts, applyTargets.Arm, arming after apply
internal/instrument/admin_fault.go       faultAdminHandler(..., arm func(*evaluator.Decision))
internal/interceptor/interceptor.go      IsInlineFault exported
internal/metrics/metrics.go              ProxyOverhead, ProxyInflight
internal/proxy/config.go                 Config, EgressListener, IngressListener
internal/proxy/egress.go                 egress handler (rewrite, extract, overhead, hijack error handler)
internal/proxy/ingress.go                ingress handler (inject, rewrite)
internal/proxy/toxic.go                  PolicyTable, Policy, PolicyFault, InlineToxicFactory
internal/proxy/toxic_transport.go        ToxicTransport, HijackError, body pipes
internal/proxy/proxy.go                  Proxy: New, Start, Shutdown, addresses
internal/atroposd/config.go              env + YAML → atroposd.Config
internal/atroposd/daemon.go              Run(ctx, Config) error
cmd/atroposd/main.go                     flags/env → Run
```

---

### Task 1: Shared middleware in `internal/instrument` (+ inline error short-circuit)

**Files:**
- Create: `internal/instrument/middleware.go`, `internal/instrument/middleware_test.go`
- Modify: `middleware.go` (root becomes thin wrappers), `serve.go`

- [ ] **Step 1: Write the failing test for the error short-circuit (ingress and egress)**

```go
package instrument

func TestIngressMiddleware_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "err", Point: evaluator.Ingress,
		Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline,
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: 503, Message: "injected fault"}},
	}), nil)
	var reached atomic.Bool
	h := IngressMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached.Store(true) }), "svc", ic)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), "injected fault") {
		t.Fatalf("code=%d body=%q, want 503 + message", rec.Code, rec.Body.String())
	}
	if reached.Load() {
		t.Fatal("handler must not run when an inline error fault fires")
	}
}

func TestEgressTransport_ErrorFaultShortCircuits(t *testing.T) {
	ic := interceptor.New(evaluator.NewStaticEvaluator(evaluator.StaticRule{
		Name: "err", Point: evaluator.Egress,
		Decision: evaluator.Decision{Name: "err", Mode: evaluator.Inline,
			Fault: &inline.Error{FaultConfig: fault.FaultConfig{Duration: 1}, StatusCode: 502, Message: "boom"}},
	}), nil)
	var live atomic.Int32
	base := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		live.Add(1)
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	resp, err := EgressTransport(base, func() *interceptor.Interceptor { return ic }).RoundTrip(httptest.NewRequest("GET", "http://dep/x", nil))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 502 || !strings.Contains(string(body), "boom") || live.Load() != 0 {
		t.Fatalf("status=%d body=%q live=%d", resp.StatusCode, body, live.Load())
	}
	if resp.Header.Get("X-Atropos-Fault") != "error" {
		t.Fatal("synthetic error response must be marked X-Atropos-Fault: error")
	}
}
```

- [ ] **Step 2: Run to verify failure** — `go test ./internal/instrument/ -run 'ShortCircuits' -v` → FAIL (`undefined: IngressMiddleware`).

- [ ] **Step 3: Implement `internal/instrument/middleware.go`**

Move root `ingressMiddleware`/`egressTransport`/`statusRecorder`/`roundTripperFunc` here, exported as `IngressMiddleware(next http.Handler, serviceName string, i *interceptor.Interceptor) http.Handler` and `EgressTransport(base http.RoundTripper, resolve func() *interceptor.Interceptor) http.RoundTripper`. The interceptor's own `IngressMiddleware`/`EgressTransport` (in `internal/interceptor/middleware.go`) gain the short-circuit: after waiting on an inline handle, read the result (`<-cr.Handle.Done()` returns it) and if `inline.IsErrorResult(res)` is non-nil, at ingress write `w.Header().Set("X-Atropos-Fault","error")`, `http.Error(w, detail.StatusText(), detail.StatusCode)` and return; at egress return a synthetic `*http.Response` (status, `text/plain` body = `StatusText()`, header `X-Atropos-Fault: error`) without calling `base.RoundTrip`. The gRPC interceptors keep today's behaviour (out of scope).

- [ ] **Step 4: Root wrappers** — root `middleware.go` becomes: `func ingressMiddleware(next, name, i) http.Handler { return instrument.IngressMiddleware(next, name, i) }`, `func egressTransport(base, resolve) http.RoundTripper { return instrument.EgressTransport(base, resolve) }`, public `EgressTransport(base)` unchanged, `roundTripperFunc` kept for root tests. Root and instrument suites green.

- [ ] **Step 5: Commit** — `feat(m1): shared IngressMiddleware/EgressTransport in internal/instrument; inline error faults short-circuit`

---

### Task 2: Proxy metrics

**Files:** `internal/metrics/metrics.go`

- [ ] Add and register:

```go
	// ProxyOverhead is the time a request spends inside the atroposd proxy
	// excluding the upstream round trip (G-proxy, design §5.13).
	ProxyOverhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "atropos_proxy_overhead_seconds",
		Help:    "Time spent inside the atroposd proxy, excluding the upstream round trip.",
		Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25},
	}, []string{"point"})
	ProxyInflight = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "atropos_proxy_inflight",
		Help: "Requests currently inside the atroposd proxy.",
	}, []string{"point"})
	RulesUnsupportedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "atropos_rules_unsupported_total",
		Help: "Rules skipped because this host cannot execute their fault category.",
	}, []string{"category"})
```

- [ ] Commit — `feat(m1): proxy overhead/inflight and rules-unsupported series`

---

### Task 3: `internal/proxy` egress listener (explicit mode)

**Files:** `internal/proxy/config.go`, `internal/proxy/proxy.go`, `internal/proxy/egress.go`, `internal/proxy/egress_test.go`

- [ ] **Step 1: Tests (write first)**
  - `TestEgress_ExplicitForwardsAndRewritesHost`: fake upstream records `r.Host` and the path; a request to the egress listener with `Host: localhost:NNN` arrives upstream with `Host` = the configured upstream `host:port` and the same path/query; response body/status pass through.
  - `TestEgress_InlineLatencyApplies`: install an egress latency rule on the core (`c.host.SetRules` through the instrument test seam is not available from `proxy`, so drive `/admin/rules` on a `ControlSurface` server); a request through the listener takes ≥ the delay.
  - `TestEgress_CacheKeyIdenticalToInProcess` (golden, INV-2): passthrough rule with `canonical_v2` context; request `GET /api/items?b=2&a=1` with `Accept: application/json` once through `instrument.EgressTransport(base, ...)` directly (client-side request to `http://<upstream>/...`) and once through the listener; both responses carry `X-Atropos-Cache-Key` and the values are equal.
  - `TestEgress_ExtractsTraceContextAndBaggage`: send `traceparent` + `baggage: atropos.workflow=browse`; a rule labelled `atropos.cachebox.workflow=browse` matches (proves baggage → labels), and the upstream receives a `traceparent` whose trace id equals the incoming one and whose parent span id differs (a client span was created under the caller's span).
- [ ] **Step 2: Implement**

```go
// config.go
type EgressListener struct {
	Listen   string // "127.0.0.1:19090"
	Upstream string // "productcatalogservice:3550" (explicit mode)
}
type IngressListener struct {
	Listen   string // ":16006"
	Upstream string // "127.0.0.1:8080" — the app
}
type Config struct {
	Service string
	Resolve func() *interceptor.Interceptor
	Egress  []EgressListener
	Ingress *IngressListener
	Base    http.RoundTripper // nil → http.DefaultTransport clone; the daemon passes ToxicTransport
	Logger  *slog.Logger
}
```

`egress.go`: `newEgressHandler(cfg Config, l EgressListener) http.Handler`:

```go
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.Out.URL.Scheme = "http"
			pr.Out.URL.Host = l.Upstream
			pr.Out.Host = l.Upstream // explicit mode: the app dialed localhost; the key must see the real host (design §5.2)
		},
		Transport:    instrument.EgressTransport(cfg.base(), cfg.Resolve), // + overhead accounting wrapper
		FlushInterval: 0,
		ErrorHandler: hijackingErrorHandler(cfg.Logger),
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ov := &overhead{}
		ctx = withOverhead(ctx, ov)
		metrics.ProxyInflight.WithLabelValues("egress").Inc()
		defer metrics.ProxyInflight.WithLabelValues("egress").Dec()
		start := time.Now()
		rp.ServeHTTP(w, r.WithContext(ctx))
		metrics.ProxyOverhead.WithLabelValues("egress").Observe((time.Since(start) - ov.upstream()).Seconds())
	})
```

The overhead wrapper is a `RoundTripper` inserted as the innermost base: it times `base.RoundTrip` and stores the duration in the per-request `overhead` holder from the context (so the measured overhead excludes upstream time but includes evaluation, keying, buffering and copying). `hijackingErrorHandler` is a stub in this task (`http.Error(w, err.Error(), 502)`); Task 5 fills it.

`proxy.go`: `type Proxy struct{...}`; `New(cfg Config) *Proxy`; `(*Proxy).Start(ctx) error` listens on every configured address (`net.Listen("tcp", …)`) and serves with `http.Server{Handler, ReadHeaderTimeout: 10s}`; `EgressAddrs() []string`, `IngressAddr() string` (bound addresses, for tests with `:0`); `Shutdown(ctx)` shuts every server down.

- [ ] **Step 3: Tests green; commit** — `feat(m1): internal/proxy egress listener (explicit mode) with in-process-identical transport`

---

### Task 4: Ingress listener

**Files:** `internal/proxy/ingress.go`, `internal/proxy/ingress_test.go`

- [ ] Tests: `TestIngress_ForwardsToAppPreservingHost`; `TestIngress_InlineLatencyApplies` (ingress latency rule via `/admin/rules`); `TestIngress_InjectsSidecarSpanContext` (with an in-memory exporter installed as the global tracer provider: the app receives `traceparent` whose parent-id is the sidecar's SERVER span id, and that span's parent is the incoming `traceparent`'s span id).
- [ ] Implement `newIngressHandler(cfg, l)`: `rp := &httputil.ReverseProxy{Rewrite: func(pr){ pr.Out.URL.Scheme="http"; pr.Out.URL.Host=l.Upstream; pr.Out.Host=pr.In.Host; otel.GetTextMapPropagator().Inject(pr.In.Context(), propagation.HeaderCarrier(pr.Out.Header)) }, Transport: plain}`; wrap: `instrument.IngressMiddleware(rp, cfg.Service, cfg.Resolve())` plus the inflight/overhead accounting around it (point `ingress`).
- [ ] Commit — `feat(m1): internal/proxy ingress listener with span injection`

---

### Task 5: Network toxics as an inline transport, policy table, eager arming

**Files:** `internal/proxy/toxic.go`, `internal/proxy/toxic_transport.go`, `internal/proxy/toxic_test.go`, `internal/instrument/{decode.go,register.go,core.go,admin_fault.go,control_surface.go}`, `internal/interceptor/interceptor.go`

- [ ] **Step 1: Tests (write first)**
  - `TestToxicTransport_LatencyDownstreamDelaysResponse`: policy `{Target: upstreamHost, Direction: Downstream, Scope: 1, Toxics: [Latency 150ms]}` armed; a `GET` through `ToxicTransport{Base: real transport}` completes in ≥150ms and the body is intact.
  - `TestToxicTransport_UnmatchedHostPassesThrough`: policy for another host; no delay.
  - `TestToxicTransport_BlackholeReturnsHijackError`: policy with `Blackhole`; `RoundTrip` returns `*HijackError{Mode: HijackBlackhole}` and never calls base.
  - `TestToxicTransport_RSTReturnsHijackErrorWithResponse`: policy with `RST{AfterBytes: 1}`; `RoundTrip` returns `*HijackError{Mode: HijackRST, Resp: resp, AfterBytes: 1}` and base was called once.
  - `TestEgress_RSTResetsClientConnection`: through the real egress listener: the app-side `http.Client` gets a connection error (`EOF` / `connection reset`) instead of a 502.
  - `TestEgress_BlackholeHoldsUntilClientTimeout`: client with 300ms timeout errors with a deadline error; upstream never called.
  - `TestPolicyFault_ArmsForDurationAndDisarmsOnStop`: `PolicyFault.Start` arms, `Handle.Stop()` disarms (table no longer matches), result delivered.
  - `TestEagerArm_NetworkRuleAppliesWithoutTraffic` (instrument): a core built with `EagerArm: true` and `DecodeOptions: [WithInlineToxicHost(factory)]`; `Apply` of a `RuleSync` with a background network `host=inline` rule leaves the factory-armed table matching immediately, and applying an empty set disarms it.
- [ ] **Step 2: Implement `toxic.go`**

```go
type Policy struct {
	Name      string            // rule/slot name (registry key)
	Target    string            // "host:port" or "host"; matched against req.URL.Host
	Direction network.Direction
	Scope     float64           // fraction of requests, 0 → 1.0
	Toxic     network.Toxic
}
type PolicyTable struct{ mu sync.RWMutex; armed map[string]*armedPolicy; rng *rand.Rand }
func NewPolicyTable() *PolicyTable
func (t *PolicyTable) Arm(ctx context.Context, p Policy)   // until ctx done
func (t *PolicyTable) Disarm(name string)
func (t *PolicyTable) Match(host string) (Policy, context.Context, bool) // rolls scope; first armed policy whose Target equals host or host's hostname
// PolicyFault implements fault.Fault: Start arms p in table until Duration elapses or Stop; Result.Detail = PolicyDetail{Requests, Affected}.
type PolicyFault struct{ fault.FaultConfig; Table *PolicyTable; Policy Policy }
// InlineToxicFactory is the instrument.WithInlineToxicHost hook.
func InlineToxicFactory(table *PolicyTable) func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error)
```

- [ ] **Step 3: Implement `toxic_transport.go`**

```go
type HijackMode int
const ( HijackRST HijackMode = iota + 1; HijackBlackhole )
// HijackError tells the egress handler to take over the client connection.
type HijackError struct {
	Mode          HijackMode
	Resp          *http.Response // RST: the upstream response to forward until the trigger
	AfterBytes    int64
	AfterDuration time.Duration
	Hold          context.Context // blackhole: hold until done
}
func (e *HijackError) Error() string
type ToxicTransport struct{ Base http.RoundTripper; Table *PolicyTable }
func (t *ToxicTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	p, pctx, ok := t.Table.Match(r.URL.Host)
	if !ok { return t.Base.RoundTrip(r) }
	switch tox := p.Toxic.(type) {
	case *network.Blackhole:
		return nil, &HijackError{Mode: HijackBlackhole, Hold: pctx}
	case *network.RST:
		resp, err := t.Base.RoundTrip(r)
		if err != nil { return nil, err }
		return nil, &HijackError{Mode: HijackRST, Resp: resp, AfterBytes: tox.AfterBytes, AfterDuration: tox.AfterDuration}
	}
	if p.Direction == network.Upstream && r.Body != nil && r.Body != http.NoBody {
		r.Body = pipeThrough(pctx, r.Body, p.Toxic) // request bytes through the toxic
	}
	resp, err := t.Base.RoundTrip(r)
	if err != nil || p.Direction != network.Downstream { return resp, err }
	resp.Body = pipeThrough(pctx, resp.Body, p.Toxic)
	resp.ContentLength = -1 // stream: ReverseProxy flushes each write, so delays reach the client as they happen
	resp.Header.Del("Content-Length")
	return resp, nil
}
// pipeThrough runs toxic.Pipe(ctx, src, pw) on a goroutine and returns the read end.
func pipeThrough(ctx context.Context, src io.ReadCloser, toxic network.Toxic) io.ReadCloser
```

`hijackingErrorHandler` (egress.go): `if errors.As(err, &hj)`: `conn, buf, err := w.(http.Hijacker).Hijack()`; blackhole → `go io.Copy(io.Discard, conn)`; `<-hj.Hold.Done()`; close. RST → `w := &rstWriter{conn, after: hj.AfterBytes, timer}`; `hj.Resp.Write(w)`; on trigger `conn.(*net.TCPConn).SetLinger(0)`; `conn.Close()`. Any other error → 502 with the error text, as before.

- [ ] **Step 4: Instrument hooks**
  - `interceptor.IsInlineFault` exported (rename).
  - `decode.go`: `type DecodeOption = decodeOption` exported alias plus `WithInlineToxicHost(fn InlineToxicHost) DecodeOption`; `decodeNetworkFault` case `"inline"`: if `cfg.inlineToxic == nil` keep the v6 error, else `toxic, err := decodeNetworkToxic(faultType, params); return cfg.inlineToxic(ruleName, env, toxic, baseCfg)` — thread the rule name through `decodeCompiledRule` → `decodeFault(f, cfg, name)`; admin faults use the slot id.
  - `register.go`: `applyTargets.DecodeOpts []decodeOption` (appended to the resolver option), `applyTargets.Arm func(*evaluator.Decision)`; after `applyRuleSet` and after each `applyActiveFault`, when `Arm != nil`, call it for every decision whose fault is non-nil and not inline.
  - `core.go`: `CoreConfig.EagerArm bool`, `CoreConfig.DecodeOptions []DecodeOption`; `Core.Arm(d *evaluator.Decision)` = `c.ic.StartFault(context.Background(), evaluator.Request{Point: ...}, d)` (logs errors); `targets()` fills `DecodeOpts` and `Arm` (nil unless EagerArm).
  - `admin_fault.go`: `faultAdminHandler(eval, reg, resolve, opts []decodeOption, arm func(*evaluator.Decision))`; POST builds the fault with the options and arms eagerly when `arm != nil`. `ControlSurface` passes the core's options and arm hook.
- [ ] **Step 5: Tests green (proxy + instrument + root); commit** — `feat(m1): inline network toxics via ToxicTransport, policy table, hijack reproduction, eager arming`

---

### Task 6: `internal/atroposd` + `cmd/atroposd`

**Files:** `internal/atroposd/config.go`, `internal/atroposd/config_test.go`, `internal/atroposd/daemon.go`, `internal/atroposd/daemon_test.go`, `cmd/atroposd/main.go`

- [ ] **Step 1: Config**

```go
type Config struct {
	Service        string            `yaml:"service"`
	Version        string            `yaml:"version"`
	Environment    string            `yaml:"environment"`
	ManteionURL    string            `yaml:"manteion_url"`
	AdvertiseAddr  string            `yaml:"advertise_addr"`  // default: <first non-loopback IPv4>:<control port>
	InstanceID     string            `yaml:"instance_id"`
	ControlListen  string            `yaml:"control_listen"`  // default ":16020"
	IngressListen  string            `yaml:"ingress_listen"`  // default ":16006"; "" disables
	InboundUpstream string           `yaml:"inbound_upstream"` // default "127.0.0.1:8080"
	Capture        string            `yaml:"capture"`         // "explicit" (M1) | "transparent" (M2)
	Explicit       map[int]string    `yaml:"explicit"`        // listen port → upstream host:port
	Routes         []wire.Route      `yaml:"routes"`
}
func Load(path string) (Config, error)   // YAML file (optional) then env overlay: ATROPOS_SERVICE, MANTEION_URL, MANTEION_ADVERTISE_ADDR, MANTEION_INSTANCE_ID, MANTEION_SERVICE_VERSION, ATROPOSD_CONTROL_ADDR, ATROPOSD_INGRESS_ADDR, ATROPOSD_INBOUND_UPSTREAM, ATROPOSD_CAPTURE
func (c Config) Validate() error         // service required; capture explicit needs ≥1 explicit entry or an ingress listener; transparent → "not supported until M2"
```

Tests: YAML + env precedence; validation errors.

- [ ] **Step 2: `Run`**

```go
// Run hosts the instrument out of process until ctx is cancelled.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) error
```
Order: validate → `InitTelemetry` → `NewPolicyTable` → `NewCore{EagerArm: true, DecodeOptions: [WithInlineToxicHost(InlineToxicFactory(table))]}` → `ConnectManteion` (fatal if configured-but-unreachable; advertise addr defaults to `<ip>:<control port>`) → `ControlSurface` on `ControlListen` → `proxy.New{Base: &ToxicTransport{Base: transport, Table: table}}` → `Start` → wait for ctx → shutdown in order: proxy (5s drain) → control server → `mc.Close` → `core.Stop` → OTel flush. For tests, `Run` accepts `cfg.ControlListen = "127.0.0.1:0"` and exposes bound addresses through an `Addrs` channel/callback (`cfg.OnReady func(Addrs)`).

Also register-time: `ConnectConfig` gains `Address` so the daemon can advertise `ip:port` (today `defaultManteionConfig` uses `MANTEION_ADVERTISE_ADDR` or a bare IP); the in-process host keeps its current default.

- [ ] **Step 3: End-to-end test (offline, mirrors `serve_test`)**

`TestDaemon_RecordPreloadFreezeReplay`: fake upstream; `Run` with one explicit egress listener → upstream, control on `:0`; via the control address: install passthrough rule with context → one request through the egress listener is recorded (`X-Atropos-Cache-Key`, fidelity `record_enqueued=1`); switch to replay without preload → 503 fail-closed, `replay_misses=1`, upstream not called; preload begin/chunk/commit an entry whose key is derived exactly as `TestServe_PreloadCommitThenReplayHit` does (client-side request to `http://<upstream>/api/items`) → replay hit `200 {"replayed":true}` with `X-Atropos-Cache-Mode: replay`, upstream call count unchanged. Plus `TestDaemon_ManteionConfiguredButUnreachableFails` and `TestDaemon_IngressLatencyThroughSidecar`.

- [ ] **Step 4: `cmd/atroposd/main.go`** — flags `-config` (default `$ATROPOSD_CONFIG` or `/etc/atropos/atroposd.yaml` if present), `-print-config`; signal handling (SIGINT/SIGTERM → cancel); exit 1 with the error on failure.

- [ ] **Step 5: `go build ./... && go vet ./... && go test ./...`; `GOOS=linux go build ./cmd/atroposd`; commit** — `feat(m1): atroposd daemon (explicit mode): config, Run, main, end-to-end record→preload→replay test`

---

### Task 7: Docs

- [ ] README: new section "atroposd: the out-of-process host (explicit mode)" with the env/YAML example, the `ATROPOS_HOST=sidecar` note (guard lands in M2), and what M1 does not do (transparent capture, resource faults, gRPC, hook API). AGENTS.md: one paragraph. Design doc §6: mark M0 done, M1 done with the commit range. Commit — `docs(m1): atroposd explicit-mode host`.

---

## Self-review

**Spec coverage (design §5, M1 row):** egress + ingress L7 listeners (Tasks 3–4), inline faults (via shared middleware, Task 1), cache-box (same transport, Task 3 golden test), network toxics on upstream connections realised as the inline `ToxicTransport` with client-leg reproduction (Task 5, design §5.6), control surface on 16020 (Task 6), hook API deferred (§7), overhead metrics (Task 2, §5.13 G-proxy), eager arming (Task 5, §3.D1), `host=proxy` still rejected without a resolver (unchanged), resource faults rejected in the daemon (the daemon passes no node-agent capability; the per-rule skip + counter lands with the capability field in M2).

**Placeholder scan:** none.

**Type consistency:** `instrument.EgressTransport(base, resolve)` and `IngressMiddleware(next, service, ic)` (Task 1) are what Tasks 3–4 call; `PolicyTable`/`Policy`/`PolicyFault`/`InlineToxicFactory`/`ToxicTransport`/`HijackError` (Task 5) are what Task 6's `Run` wires; `CoreConfig.EagerArm`/`DecodeOptions` (Task 5) are what `Run` sets.

---

## Execution notes (2026-09-24)

- Commits: `39ac174` (Task 1), `8419e57` (Task 2), `985bfc7` (Tasks 3–4), `db477ea` (Task 5), `97aaf88`
  (Task 6), docs in the following commit.
- Task 3 surfaced a key-identity hazard the golden test caught by construction: Go's transport adds
  `Accept-Encoding: gzip` on the wire, which canonical_v2 folds into the key at the sidecar but never
  in-process. The egress rewrite drops exactly that auto-added value (§5.2 note in code).
- Task 3 also surfaced a shared-transport gap: otelhttp takes the tracer provider of the span in the
  context, and an extracted remote span resolves to the no-op provider, so no child span was minted
  and the caller's span id was re-emitted. `instrument.EgressTransport` now pins the global provider.
- Task 5 changed the toxic mechanism from the planned dialer wrapper to a request-level
  `ToxicTransport` (stream toxics through `Pipe` on request/response bodies; rst and blackhole as
  `HijackError` reproduced on the client connection). Scope is therefore per request, which is what
  the wire format documents for `host=inline`. Direction defaults to downstream for inline toxics.
- The daemon config uses an `egress:` list (`listen`/`upstream`) instead of the planned port map so
  bind addresses are explicit and tests can use `:0`; `ATROPOSD_EGRESS` carries `listen=upstream`
  pairs.
