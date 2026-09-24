# atroposd M2: Transparent Capture, Capabilities, Fleet Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the sidecar deployable on the Go fleet without touching a service's dependency addresses: iptables-redirected (transparent) capture with `SO_ORIGINAL_DST`, a raw-TCP relay for non-HTTP dependencies, a startup capture self-check, the `host`/`capabilities` register fields with a per-rule unsupported skip, the `ATROPOS_HOST=sidecar` guard that makes an embedded `Serve` passive, and the deploy assets (init container, sidecar image, kustomize patch). Design §5.2, §5.3, §5.10, §5.12, §5.13 (M2 row of §6).

**Architecture:** In transparent mode the init container REDIRECTs the app's outbound TCP (except the proxy's own uid, loopback, and an exclude list) to one egress port and the app's inbound ports to one ingress port. A wrapping `net.Listener` recovers each accepted connection's original destination: connections to configured raw-TCP ports (redis, kafka, postgres) are relayed at L4 (with armed toxics applied per connection) without ever reaching the HTTP server; everything else is served by the same reverse-proxy handlers as explicit mode, with `URL.Host` = original destination and the `Host` header preserved. Capture is asserted at startup (marker file written by the init container) before the instance registers: no capture, no instance. Hosts advertise `host` and `capabilities` at registration; rules whose category a host cannot execute are skipped per rule and counted, never allowed to wedge a sync.

**Tech Stack:** Go 1.25, `golang.org/x/sys/unix` (already an indirect dependency), iptables (nat REDIRECT), kustomize strategic-merge patch.

**Behaviour changes to the in-process host:** (1) `Serve` returns a tracing-only handler and skips registration when `ATROPOS_HOST=sidecar` (§5.12). (2) The register payload carries `host: "in-process"` and `capabilities: [inline, network, resource, cachebox]` (additive; today's manteion ignores unknown fields). (3) `/atropos/health` carries `host` and `capture`. Nothing else: the in-process host advertises every category, so the unsupported-rule skip never fires there.

---

## File structure

```
internal/wire/register.go                RegisterRequest.Host, .Capabilities
internal/wire/health.go                  HealthStatus.Host, .Capture
internal/instrument/capabilities.go      Cap* constants, AllCapabilities, UnsupportedFaultError, WithCapabilities
internal/instrument/decode.go            capability check in decodeFault; per-rule skip in decodeCompiledRules
internal/instrument/register.go          applyActiveFault skips unsupported (counted)
internal/instrument/core.go              CoreConfig.Host, .Capabilities
internal/instrument/manteion.go          manteionConfig.host/.capabilities → register payload
internal/instrument/health.go            HealthHandler(c, host, capture)
internal/instrument/control_surface.go   ControlConfig.Host, .Capture
serve.go                                 ATROPOS_HOST=sidecar passive mode; host/capabilities
internal/proxy/origdst.go                parseSockaddrIn (pure), OriginalDstFunc
internal/proxy/origdst_linux.go          platformOriginalDst via SO_ORIGINAL_DST
internal/proxy/origdst_other.go          stub returning ErrOriginalDstUnsupported
internal/proxy/transparent.go            TransparentEgress/Ingress config, splitListener, origDstConn, handlers
internal/proxy/relay.go                  raw-TCP relay with armed toxics
internal/atroposd/config.go              capture: transparent fields, capture marker
internal/atroposd/daemon.go              transparent wiring, marker check, capabilities
deploy/sidecar/{Dockerfile,Dockerfile.init,init.sh,README.md}
deploy/sidecar/kustomize/recommendationservice-sidecar.yaml
```

---

### Task 1: Register capabilities and the per-rule unsupported skip

**Files:** `internal/wire/register.go`, `internal/instrument/capabilities.go`, `internal/instrument/decode.go`, `internal/instrument/register.go`, `internal/instrument/core.go`, `internal/instrument/manteion.go`, `serve.go`, `internal/atroposd/daemon.go`; tests `internal/instrument/capabilities_test.go`, `internal/instrument/routes_test.go` (extend)

- [ ] **Step 1: Failing tests**
  - `TestRegister_CarriesHostAndCapabilities`: a `ManteionClient` with `cfg.host="sidecar"`, `cfg.capabilities=[inline network cachebox]` posts a register body whose JSON has `"host":"sidecar"` and `"capabilities":["inline","network","cachebox"]`; with both unset the keys are absent (omitempty).
  - `TestDecode_UnsupportedCategoryIsSkippedAndCounted`: `decodeCompiledRules` with `WithCapabilities(CapInline, CapCacheBox)` over [inline latency, resource cpu, cachebox replay] returns two rules, and `metrics.RulesUnsupportedTotal{category="resource"}` increased by one.
  - `TestApply_UnsupportedActiveFaultDoesNotWedgeSync`: an `active_faults` entry with category `resource` under capabilities [inline] applies without error and arms no slot.
  - `TestDecode_NoCapabilitiesMeansEverythingSupported`: without the option, the same set decodes all three (in-process behaviour unchanged).
- [ ] **Step 2: Implement**

```go
// capabilities.go
const (
	CapInline   = "inline"
	CapNetwork  = "network"
	CapResource = "resource"
	CapCacheBox = "cachebox"
)
// AllCapabilities is what the in-process host advertises.
func AllCapabilities() []string { return []string{CapInline, CapNetwork, CapResource, CapCacheBox} }
// UnsupportedFaultError says the host cannot execute a fault category.
type UnsupportedFaultError struct{ Category, FaultType string }
func (e *UnsupportedFaultError) Error() string
// WithCapabilities restricts decoding to the given fault categories.
func WithCapabilities(caps ...string) DecodeOption
```

`decodeFault`: after the envelope check, `if cfg.capabilities != nil && !cfg.capabilities[f.EffectiveCategory()] { return nil, &UnsupportedFaultError{...} }`. `decodeCompiledRules`: on `errors.As(err, &unsup)` log at WARN, `metrics.RulesUnsupportedTotal.WithLabelValues(unsup.Category).Inc()`, `continue`. `applyActiveFault`: same skip (return nil). `wire.RegisterRequest` gains `Host string json:"host,omitempty"` and `Capabilities []string json:"capabilities,omitempty"`; `manteionConfig` gains `host`, `capabilities`; `register()` sends them; `CoreConfig.Host`/`.Capabilities` feed `ConnectManteion` and the decode options. `Serve`: `Host: "in-process", Capabilities: AllCapabilities()`. Daemon: `Host: "sidecar", Capabilities: [inline, network, cachebox]`.

- [ ] **Step 3: Suites green; commit** — `feat(m2): host/capabilities at registration; unsupported rules are skipped per rule and counted`

---

### Task 2: Fleet guard and health provenance

**Files:** `serve.go`, `serve_test.go`, `internal/wire/health.go`, `internal/instrument/health.go`, `internal/instrument/control_surface.go`, `health.go` (root), `internal/atroposd/daemon.go`

- [ ] **Step 1: Failing tests**
  - `TestServe_SidecarModeIsPassive` (root): with `ATROPOS_HOST=sidecar` and `MANTEION_URL` pointing at a closed port, `Serve` succeeds, serves the business handler with tracing, mounts no control surface (`/admin/fault` → 404), and `Health()` reports `offline` with `host: "in-process"`.
  - `TestHealthHandler_StampsHostAndCapture` (instrument): `HealthHandler(nil, "sidecar", "transparent")` returns `{"status":"offline","host":"sidecar","capture":"transparent"}`.
- [ ] **Step 2: Implement** — `wire.HealthStatus` gains `Host`, `Capture` (omitempty). `HealthHandler(c, host, capture)`; `ControlConfig.Host`, `.Capture`. `Serve`: read `ATROPOS_HOST`; when `sidecar`: init telemetry, log once, return `ingressMiddleware(cfg.Handler, service, currentInterceptor())` (or `http.NotFoundHandler()` when Handler is nil) and the OTel shutdown; no core, no registration, no control surface. Root `Health()` stamps `Host: "in-process"`.
- [ ] **Step 3: Commit** — `feat(m2): ATROPOS_HOST=sidecar makes Serve passive; health carries host/capture`

---

### Task 3: Transparent listeners in `internal/proxy`

**Files:** `internal/proxy/origdst.go`, `origdst_linux.go`, `origdst_other.go`, `transparent.go`, `relay.go`; tests `origdst_test.go`, `transparent_test.go`

- [ ] **Step 1: Failing tests**
  - `TestParseSockaddrIn`: 16 bytes `{AF_INET (native endian), port 0x0d, 0xde (3550), 10.42.0.7, zeros}` → `"10.42.0.7:3550"`; wrong family → error.
  - `TestTransparentEgress_ForwardsToOriginalDstPreservingHost`: `Config.OriginalDst` injected to return the fake upstream's address; the app's request carries `Host: productcatalogservice:3550`; the upstream receives that Host and the path; cache key equals the in-process key for `http://productcatalogservice:3550/...` (Host is the key's host, not the dial address). *Note:* in-process `URL.Host` is the address the app dialed (`productcatalogservice:3550`), identical to the transparent `Host` — the golden check uses a client-side request with that URL and asserts key equality against the proxied response's key.
  - `TestTransparentEgress_LoopGuard`: original dst = the listener's own address → 502, upstream never called.
  - `TestTransparentEgress_MissingOriginalDstIs502`: injected error → 502.
  - `TestTransparentIngress_RoutesByOriginalPort`: original dst port 8080 → forwarded to `AppHost:8080`; with `AllowedPorts: [8080]` a port 9999 dst → 502.
  - `TestRawRelay_EchoesAndAppliesLatency`: a TCP echo upstream on a port listed in `RawTCPPorts`; the client connects to the egress listener (origDst injected = echo addr); bytes echo; with a downstream latency policy armed for that target, the echo takes ≥ delay.
- [ ] **Step 2: Implement**

```go
// origdst.go
type OriginalDstFunc func(net.Conn) (string, error)
var ErrOriginalDstUnsupported = errors.New("proxy: original destination lookup requires linux")
func parseSockaddrIn(b []byte) (string, error) // AF_INET only; IPv6 is a follow-up
// origdst_linux.go: platformOriginalDst uses unix.GetsockoptIPv6Mreq(fd, unix.IPPROTO_IP, unix.SO_ORIGINAL_DST) on the raw fd
// transparent.go
type TransparentEgress struct { Listen string; RawTCPPorts []int }
type TransparentIngress struct { Listen string; AppHost string; AllowedPorts []int }
// Config gains: TransparentEgress *TransparentEgress; TransparentIngress *TransparentIngress; OriginalDst OriginalDstFunc (nil → platform)
type origDstConn struct { net.Conn; dst string }
// splitListener wraps the bound listener: Accept resolves the original destination; raw-TCP ports are relayed and never returned; others are returned as *origDstConn. http.Server.ConnContext stores dst in the connection context.
// newTransparentEgressHandler: like the explicit handler, but URL.Host = dst from the context, Host preserved; 502 when dst is missing or equals the listener's own address.
// newTransparentIngressHandler: upstream = AppHost:<dst port>, 502 when the port is not allowed.
// relay.go: relay(ctx, client net.Conn, dst string, table *PolicyTable) — dial dst; if a policy matches (scope rolled per connection) apply it: stream toxic on the chosen direction, RST via rstWriter on the client leg, blackhole = hold; else bidirectional copy. Closes both ends when either side or the policy context ends.
```

- [ ] **Step 3: Suites green (incl. `GOOS=linux go build ./...`); commit** — `feat(m2): transparent capture: original-destination listeners, raw-TCP relay`

---

### Task 4: Daemon wiring for transparent mode

**Files:** `internal/atroposd/config.go`, `daemon.go`, tests

- [ ] Config: `egress_listen` (default `:16001`), `inbound_ports` (default `[]` = no inbound capture), `raw_tcp_ports`, `capture_marker` (default `/var/run/atropos/capture.ok`), `app_host` (default `127.0.0.1`); env `ATROPOSD_EGRESS_ADDR`, `ATROPOSD_INBOUND_PORTS`, `ATROPOSD_RAW_TCP_PORTS`, `ATROPOSD_CAPTURE_MARKER`. `Validate`: transparent no longer rejected; needs `egress_listen`. `Run`: in transparent mode, before binding anything, require the marker file unless the test hook `OriginalDst` is set (no capture → no instance, §5.2); build `TransparentEgress`/`TransparentIngress`; `Capture` stamped on health; capabilities as Task 1. Tests: `TestRun_TransparentRequiresCaptureMarker`, `TestRun_TransparentEndToEnd` (hook-injected original dst; record → replay through the transparent listener).
- [ ] Commit — `feat(m2): atroposd transparent capture mode with startup capture check`

---

### Task 5: Deploy assets

**Files:** `deploy/sidecar/Dockerfile`, `Dockerfile.init`, `init.sh`, `README.md`, `kustomize/recommendationservice-sidecar.yaml`

- [ ] `init.sh` (run as root with NET_ADMIN): creates the `ATROPOS_OUTPUT`/`ATROPOS_INBOUND` nat chains; excludes the proxy uid (`ATROPOS_PROXY_UID`, default 1337), loopback, `ATROPOSD_EXCLUDE_OUTBOUND_PORTS` (default `4317,4318`: the OTel collector must not be proxied) and `ATROPOSD_EXCLUDE_CIDRS`; REDIRECTs remaining outbound TCP to `ATROPOSD_EGRESS_PORT` (16001) and `ATROPOSD_INBOUND_PORTS` to `ATROPOSD_INGRESS_PORT` (16006); writes the marker with the applied parameters. `sh -n init.sh` clean. The sidecar image runs as uid 1337. The kustomize file is a strategic-merge patch for one Go-port service: init container, sidecar container (`ATROPOS_SERVICE`, `MANTEION_URL`, `ATROPOSD_CAPTURE=transparent`, `ATROPOSD_INBOUND_PORTS`, resources), shared `emptyDir` at `/var/run/atropos`, and `ATROPOS_HOST=sidecar` on the app container. README: what to verify on VM1 first (kernel, cgroup v2, iptables backend, PodSecurity, k3s version for native sidecars).
- [ ] Commit — `feat(m2): sidecar deploy assets (init container, image, kustomize patch)`

---

### Task 6: Docs

- [ ] README §7: transparent mode paragraph + env; design doc §6 status; plan execution notes. Commit — `docs(m2): transparent capture`.

---

## Self-review

**Spec coverage:** §5.2 transparent capture + self-check (Tasks 3–4), explicit mode kept (M1); §5.3 register fields + per-rule skip + counter (Task 1); §5.6 raw-TCP ports get L4 toxics (Task 3 relay); §5.9 config keys (Task 4); §5.10 kustomize patch + init container (Task 5); §5.12 mixed-fleet guard (Task 2); §5.13 G-proxy metrics (M1) and capture self-check (Task 4). IPv6 original-destination lookup and the app-uid probe are named follow-ups, not silent gaps.

**Placeholder scan:** none.

**Type consistency:** `OriginalDstFunc` (Task 3) is what `atroposd.Config.OriginalDst` (Task 4) injects; `WithCapabilities`/`AllCapabilities` (Task 1) are what `Serve` and `Run` pass; `HealthHandler(c, host, capture)` (Task 2) is what `ControlSurface` calls with `ControlConfig.Host/Capture`.
