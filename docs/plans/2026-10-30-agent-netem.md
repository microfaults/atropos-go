# Node-agent `netem` mechanism Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real packet loss and kernel-level latency for a sidecar-instrumented pod: the privileged node agent enters the pod's network namespace and applies `tc netem` on the pod's egress interface, journaled and restored exactly like cgroup quotas.

**Architecture:** `faultparams` gains a `network/packet_loss` type and a `mechanism: "netem"` option on `network/latency`. The sidecar decodes such faults through a new `WithNetworkHost` hook (the twin of `WithResourceHost`) into the existing `agent.AgentFault`, so the fault's lifetime is still the sidecar's streaming `POST /v1/faults`. The agent resolves the target container's first pid, derives `/proc/<pid>/ns/net`, runs `nsenter --net=<ns> tc ...` through an injectable `Runner` (exec-stubbed in tests), journals `{id, pod_uid, netns, netns_id, dev, applied}` before the first `tc` command, and restores with `tc qdisc del dev <dev> root` on stop, on client disconnect, and at boot. A `prio`-root + `u32` filter layout scopes the fault to one dependency (`network.target`, resolved to its ClusterIP(s) by the agent); without a target, netem sits on the root and shapes the whole pod's egress. The agent reports its mechanisms on `GET /v1/capabilities`; the sidecar advertises `netem` only when the agent lists it.

**Tech Stack:** Go 1.25 module (`go 1.25.5`), stdlib only (`os/exec`, `net`, `encoding/json`); `tc` from `iproute2` and `nsenter` from `util-linux` in the agent image; kernel modules `sch_netem`, `sch_prio`, `cls_u32` on the node.

**Spec:** scope statement in the task brief (this file's "Scope" below), `docs/plans/2026-09-27-daemon-design-review-2.md` §5 (line 296, the netem row), `docs/plans/2026-09-08-atropos-daemon-design.md` §5.14 (lines 531-589, the agent's shape, own-pod check, journal contract), `docs/plans/2026-09-24-atroposd-m4-node-agent.md` (execution notes, lines 86-103: a fault lives as long as the streaming request; the journal covers an agent crash).

## Scope (from the brief)

1. `faultparams`: new network fault type `packet_loss` `{loss (percent, 0-100], correlation (percent, optional)}`; `mechanism: "netem"` accepted on network `latency` (`delay`, `jitter`); validation; catalogue docs in `AGENTS.md`.
2. Agent: a `netem` mechanism: first pid of the target container's `cgroup.procs` → `/proc/<pid>/ns/net`; `nsenter --net=<ns> tc ...`; `prio` root + `u32 match ip dst <addr>` steering matched traffic to a band carrying `netem loss X% [Y%]` / `netem delay Dms Jms`; without a target, netem on the root; egress only; journal entries restored on stop, on disconnect, at boot; interface discovery with `ip -json link`, default `eth0`.
3. Server/API: `StartRequest.Fault` with category `network` + mechanism `netem` accepted; `GET /v1/capabilities` → `{"mechanisms":["burn","quota","netem"]}`.
4. Sidecar: advertises `netem` when the agent reports it; `WithNetworkHost` routes netem faults to `agent.NetworkFactory(...)`; other network faults keep the toxic path; the in-process host rejects netem at decode with `UnsupportedFaultError`.
5. Tests: exec stub for `nsenter`/`tc`, journal round-trip, server start/stop via `startTestAgent`, decode routing; documented manual verification on a k3s node.

## Global Constraints

- Go module `go 1.25.5` (`go.mod:3`) built on a Go 1.27 host; **no new module dependencies**; `faultparams` stays stdlib-only (`faultparams/params.go:11-12`).
- After every task, all four are green from the worktree root: `gofmt -l .` (prints nothing), `go vet ./...`, `go test -count=1 ./...`, `GOOS=linux go build ./cmd/...`. Known pre-existing darwin flake unrelated to this plan: `TestStartBurn_StopEndsTheChildEarly` in `internal/agent` (passes on re-run; linux CI is the gate).
- Commits are conventional (`feat(netem): ...`, `docs(netem): ...`) and end with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Wire compatibility is additive: `internal/wire` is not modified; `mechanism` is optional on `NetworkLatency`; `packet_loss` is a new `(category, fault_type)`; `NetworkEnvelope{Host, Target, Direction, Scope}` (`internal/wire/fault.go:47-66`) is reused unchanged (`host` is ignored for netem).
- Unit tests never execute `nsenter` or `tc`: every `tc` path goes through the `Runner` interface; `ExecRunner` is wired only in `cmd/atropos-agent/main.go`.
- Semantics fixed by this plan: egress only (ingress shaping needs an `ifb` device: out of scope); IPv4 targets only; one netem fault per pod network namespace at a time; `scope` multiplies `loss` on `packet_loss` and is unsupported (must be 0 or 1) on netem `latency`; `direction` must be `""` or `"upstream"`.
- Do not touch `.claude/` or `vendor/`.

## Review Focus

Uncovered by the spec's words, pinned by tests in the owning task:

1. A target that resolves to several addresses (headless service, multi-endpoint): every IPv4 address gets its own `u32` filter, not just the first → Task 3 `TestNetem_ResolvesTargetToEveryIPv4`.
2. A second netem fault posted for a pod that already has one: refused with 409 and the first fault's qdisc untouched, instead of a `tc qdisc add` failure or a clobbered root → Task 4 `TestServer_NetemRefusesASecondFaultInTheSameNamespace`.
3. The pod restarts (or the pid is reused by another namespace) between apply and restore: restore must not run `tc` in a stranger's namespace; it drops the journal entry and succeeds → Task 3 `TestNetem_RestoreSkipsAChangedNamespace`.
4. A `tc` command fails mid-plan (filter rejected after the root qdisc exists): the root is deleted again, the journal entry removed, the namespace released, and the error carries tc's output → Task 3 `TestNetem_ApplyRollsBackWhenACommandFails`.
5. An agent that predates `GET /v1/capabilities` (404): the sidecar still delegates resource faults but does not advertise `netem`; netem rules are skipped and counted, never a sync failure → Task 6 `TestCapabilities_LegacyAgentReportsBurnAndQuota` and `TestRun_AdvertisesNetemOnlyWhenTheAgentOffersIt`.

---

## File structure

```
faultparams/params.go                 MechanismNetem, ValidateNetworkMechanism, NetworkMechanism (reads params.mechanism),
                                      NetworkLatency.Mechanism, NetworkPacketLoss
faultparams/registry.go               network/packet_loss catalogue entry
faultparams/params_test.go            coverage list, golden fixtures, mechanism tests
internal/agent/cgroup.go              FirstPID(cgroupDir)
internal/agent/netem.go               NetemSpec, NetemSpecFromRequest, netemArgs, netemPlan, pickInterface (pure: no I/O)
internal/agent/netem_apply.go         Runner, ExecRunner, RunnerFunc, NetemEntry, NetemJournal, Netem{Apply,Restore,RestoreAll}, ErrNetemBusy
internal/agent/api.go                 CapabilitiesResponse; Status.Netns/Dev
internal/agent/server.go              Server.Netem, Mechanisms(), GET /v1/capabilities, netem branch of start, startTimed helper
internal/agent/client.go              Capabilities(sock), NetworkFactory(sock, podUID, container)
internal/instrument/capabilities.go   CapNetem
internal/instrument/decode.go         NetworkHost, WithNetworkHost, netem routing in decodeFault
internal/atroposd/daemon.go           advertise netem + install WithNetworkHost when the agent lists netem
cmd/atropos-agent/main.go             -netem-journal flag, tc/nsenter lookup, boot restore, Server.Netem
deploy/agent/Dockerfile               apk add iproute2 util-linux (+ build-time check)
deploy/agent/daemonset.yaml           -netem-journal arg
AGENTS.md, README.md, deploy/agent/README.md, docs/plans/2026-09-08-atropos-daemon-design.md   docs
```

Package dependency direction stays as today: `faultparams` ← `instrument` ← `agent` ← `atroposd`. `instrument` must not import `agent`, which is why the "is this a netem fault" predicate lives in `faultparams`.

---

### Task 1: `faultparams`: `packet_loss` type and `netem` mechanism on network latency

**Files:**
- Modify: `faultparams/params.go:85-102` (mechanism constants and `ValidateMechanism`), `faultparams/params.go:184-204` (`NetworkLatency`)
- Modify: `faultparams/registry.go:25-30`
- Modify: `AGENTS.md:72-80` (params catalogue sentence), `AGENTS.md:139` (decoder support line)
- Test: `faultparams/params_test.go`

**Interfaces:**
- Produces:
  - `const MechanismNetem = "netem"` (next to `MechanismBurn`/`MechanismQuota`, `params.go:89-92`).
  - `func ValidateNetworkMechanism(faultType, m string) error` — accepts `""` and `"netem"`; error text `network <faultType>: unknown mechanism %q (want netem)`.
  - `func NetworkMechanism(faultType string, raw json.RawMessage) string` — `"netem"` for `faultType == "packet_loss"` always; otherwise the `mechanism` field of `raw` (`""` when absent/unparseable). This is the single predicate the decoder (Task 5) and the agent (Task 4) use.
  - `NetworkLatency.Mechanism string \`json:"mechanism,omitempty"\`` — `Validate` ends with `return ValidateNetworkMechanism("latency", p.Mechanism)`.
  - `type NetworkPacketLoss struct { Loss float64 \`json:"loss"\`; Correlation float64 \`json:"correlation,omitempty"\`; Mechanism string \`json:"mechanism,omitempty"\` }` — `Validate`: `Loss` in `(0,100]`, `Correlation` in `[0,100]`, then `ValidateNetworkMechanism("packet_loss", p.Mechanism)`.
  - Registry entry `{"network", "packet_loss", "Drop a percentage of packets toward the target with tc netem (node agent)", func() Param { return &NetworkPacketLoss{} }}` inserted between `latency` and `retransmit_delay` (alphabetical order is what `TestRegistryCoverage` pins).

- [ ] **Step 1: Write the failing tests**

In `faultparams/params_test.go` change the `want["network"]` list at line 14 to `{"blackhole", "drip", "latency", "packet_loss", "retransmit_delay", "rst", "throttle"}`; append to `good` (line 42) and `bad` (line 66):

```go
// good
{"network", "latency", `{"delay":"100ms","mechanism":"netem"}`},
{"network", "packet_loss", `{"loss":10}`},
{"network", "packet_loss", `{"loss":12.5,"correlation":25,"mechanism":"netem"}`},
{"network", "packet_loss", `{"loss":100}`},
// bad
{"network", "packet_loss", `{"loss":0}`, "loss 0"},
{"network", "packet_loss", `{"loss":101}`, "loss > 100"},
{"network", "packet_loss", `{"loss":10,"correlation":-1}`, "negative correlation"},
{"network", "packet_loss", `{"loss":10,"mechanism":"toxic"}`, "unknown network mechanism"},
{"network", "latency", `{"delay":"100ms","mechanism":"quota"}`, "quota is a resource mechanism"},
{"network", "rst", `{"mechanism":"netem"}`, "rst has no mechanism field"},
```

Add:

```go
func TestNetworkMechanism(t *testing.T) {
	cases := []struct {
		typ, raw, want string
	}{
		{"packet_loss", ``, "netem"},
		{"packet_loss", `{"loss":10}`, "netem"},
		{"latency", `{"delay":"1ms","mechanism":"netem"}`, "netem"},
		{"latency", `{"delay":"1ms"}`, ""},
		{"latency", `not json`, ""},
		{"rst", `{"mechanism":"netem"}`, "netem"}, // read, not validated: the decoder rejects it later
	}
	for _, c := range cases {
		if got := NetworkMechanism(c.typ, json.RawMessage(c.raw)); got != c.want {
			t.Errorf("NetworkMechanism(%s, %s) = %q, want %q", c.typ, c.raw, got, c.want)
		}
	}
	if err := ValidateNetworkMechanism("latency", "burn"); err == nil {
		t.Error("burn is not a network mechanism")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 ./faultparams/`
Expected: FAIL — `TestRegistryCoverage` reports `category network: got [... 6 types] want [... 7 types]`, `TestNetworkMechanism` does not compile (`undefined: NetworkMechanism`).

- [ ] **Step 3: Implement the constants, functions, structs and registry entry listed under Interfaces**

`NetworkMechanism` unmarshals into `struct{ Mechanism string \`json:"mechanism"\` }` and ignores the error (same style as `instrument.resourceMechanism`, `internal/instrument/decode.go:47-53`). Update the `NetworkLatency` doc comment: delay is applied by the proxy toxic by default, by `tc netem` in the pod's network namespace when `mechanism` is `netem` (node agent, sidecar only).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./faultparams/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/faultparams`

- [ ] **Step 5: Document the catalogue in `AGENTS.md`**

In the sentence at `AGENTS.md:73-80`, after the `resource cpu` example add: network packet_loss `{"loss","correlation"}` (percent; `tc netem` through the node agent, sidecar only); network latency `{"delay","jitter","mechanism"}` where `mechanism: "netem"` moves the delay from the proxy toxic to `tc netem` in the pod's network namespace; `network.scope` multiplies `loss` and must be 0 or 1 on netem latency; netem shapes pod egress only (`direction: upstream`). At `AGENTS.md:139` change the network list to `network (latency, retransmit_delay, blackhole, drip, rst, throttle; packet_loss and latency with mechanism netem only via the atroposd sidecar with the node agent)`.

- [ ] **Step 6: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add faultparams/params.go faultparams/registry.go faultparams/params_test.go AGENTS.md
git commit -m "feat(netem): faultparams packet_loss type and netem mechanism on network latency

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: agent netem spec and tc plan (pure functions)

**Files:**
- Create: `internal/agent/netem.go`
- Modify: `internal/agent/cgroup.go` (append `FirstPID` after `Attach`, line 77)
- Test: `internal/agent/netem_test.go`, `internal/agent/cgroup_test.go`

**Interfaces:**
- Consumes: `faultparams.NetworkLatency`, `faultparams.NetworkPacketLoss`, `faultparams.NetworkMechanism`, `faultparams.MechanismNetem` (Task 1); `wire.FaultRequest`, `wire.NetworkEnvelope`.
- Produces:
  - `type NetemSpec struct { FaultType string; Target string; Loss, Correlation float64; Delay, Jitter time.Duration }` — `Target` is the envelope's `host[:port]` verbatim (`""` = whole-pod egress); `Loss`/`Correlation` in percent; `Delay`/`Jitter` for latency.
  - `func NetemSpecFromRequest(req wire.FaultRequest) (NetemSpec, error)` — the one validation both the sidecar (decode time, Task 6) and the agent (start time, Task 4) run. Rules, in order: category must be `network`; `req.Network` must be non-nil; `req.Network.Direction` must be `""` or `"upstream"` (error mentions `egress only` and `ifb`); `scope := req.Network.Scope`, treated as 1 when 0; `packet_loss`: unmarshal `NetworkPacketLoss`, `Validate`, `Loss *= scope`; `latency`: unmarshal `NetworkLatency`, `Validate`, `Mechanism` must equal `netem` (error mentions `toxic`), `scope` must be 1 (error mentions `scope`), parse `Delay`/`Jitter`; any other type: error `netem supports packet_loss and latency only`.
  - `func netemArgs(spec NetemSpec) []string` — `["loss","10%","25%"]` (correlation omitted when 0; numbers via `strconv.FormatFloat(v, 'f', -1, 64)`); `["delay","100ms","20ms"]` (jitter omitted when 0; durations as `FormatFloat(float64(d)/1e6, 'f', -1, 64)+"ms"` so 1500µs renders `1.5ms`).
  - `func netemPlan(dev string, ips []net.IP, port int, spec NetemSpec) [][]string` — the `tc` argv lines (without `nsenter`), in order. Whole pod (`len(ips)==0`): one line `tc qdisc add dev <dev> root handle 1: netem <args>`. Targeted: `tc qdisc add dev <dev> root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1`, then `tc qdisc add dev <dev> parent 1:4 handle 40: netem <args>`, then per ip `tc filter add dev <dev> parent 1: protocol ip prio 1 u32 match ip dst <ip>/32 [match ip dport <port> 0xffff] flowid 1:4` (the dport match only when `port > 0`). Band 4 is reachable only through the filter: the default priomap never names it, so unmatched traffic is untouched.
  - `func pickInterface(ipLinkJSON []byte) string` — first entry of `ip -json link show` output with `link_type == "ether"`, flags containing `UP` and not `LOOPBACK`; `""` when none or unparseable.
  - `func FirstPID(cgroupDir string) (int, error)` in `cgroup.go` — first line of `<dir>/cgroup.procs`; error when the file is missing or empty (`agent: no process in cgroup <dir>`).

- [ ] **Step 1: Write the failing tests**

`internal/agent/netem_test.go`:

```go
package agent

import (
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func netReq(faultType, target, direction string, scope float64, params string) wire.FaultRequest {
	return wire.FaultRequest{Category: "network", FaultType: faultType, DurationMs: 1000,
		Network: &wire.NetworkEnvelope{Host: "inline", Target: target, Direction: direction, Scope: scope},
		Params:  json.RawMessage(params)}
}

func TestNetemSpecFromRequest(t *testing.T) {
	spec, err := NetemSpecFromRequest(netReq("packet_loss", "productcatalogservice:3550", "upstream", 0.5, `{"loss":10,"correlation":25}`))
	if err != nil || spec.Loss != 5 || spec.Correlation != 25 || spec.Target != "productcatalogservice:3550" || spec.FaultType != "packet_loss" {
		t.Fatalf("packet_loss: %+v %v (scope must multiply loss)", spec, err)
	}
	spec, err = NetemSpecFromRequest(netReq("latency", "", "", 0, `{"delay":"100ms","jitter":"20ms","mechanism":"netem"}`))
	if err != nil || spec.Delay != 100*time.Millisecond || spec.Jitter != 20*time.Millisecond || spec.Target != "" {
		t.Fatalf("latency: %+v %v", spec, err)
	}
	bad := []struct {
		name string
		req  wire.FaultRequest
		want string
	}{
		{"latency without mechanism is a toxic", netReq("latency", "", "", 0, `{"delay":"100ms"}`), "toxic"},
		{"downstream is ingress", netReq("packet_loss", "x", "downstream", 0, `{"loss":10}`), "egress only"},
		{"latency scope", netReq("latency", "", "", 0.5, `{"delay":"100ms","mechanism":"netem"}`), "scope"},
		{"other type", netReq("rst", "x", "", 0, `{"mechanism":"netem"}`), "packet_loss and latency only"},
		{"bad params", netReq("packet_loss", "x", "", 0, `{"loss":0}`), "loss"},
		{"no envelope", wire.FaultRequest{Category: "network", FaultType: "packet_loss", Params: json.RawMessage(`{"loss":1}`)}, "envelope"},
		{"not network", wire.FaultRequest{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}, "network"},
	}
	for _, c := range bad {
		if _, err := NetemSpecFromRequest(c.req); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.want)
		}
	}
}

func TestNetemArgs(t *testing.T) {
	cases := []struct {
		spec NetemSpec
		want []string
	}{
		{NetemSpec{FaultType: "packet_loss", Loss: 10, Correlation: 25}, []string{"loss", "10%", "25%"}},
		{NetemSpec{FaultType: "packet_loss", Loss: 12.5}, []string{"loss", "12.5%"}},
		{NetemSpec{FaultType: "latency", Delay: 100 * time.Millisecond, Jitter: 20 * time.Millisecond}, []string{"delay", "100ms", "20ms"}},
		{NetemSpec{FaultType: "latency", Delay: 1500 * time.Microsecond}, []string{"delay", "1.5ms"}},
	}
	for _, c := range cases {
		if got := netemArgs(c.spec); !reflect.DeepEqual(got, c.want) {
			t.Errorf("netemArgs(%+v) = %v, want %v", c.spec, got, c.want)
		}
	}
}

func TestNetemPlan_WholePodAndTargeted(t *testing.T) {
	spec := NetemSpec{FaultType: "packet_loss", Loss: 10, Correlation: 25}
	whole := netemPlan("eth0", nil, 0, spec)
	if len(whole) != 1 || strings.Join(whole[0], " ") != "tc qdisc add dev eth0 root handle 1: netem loss 10% 25%" {
		t.Fatalf("whole-pod plan = %v", whole)
	}
	ips := []net.IP{net.ParseIP("10.43.0.5"), net.ParseIP("10.43.0.6")}
	got := netemPlan("eth0", ips, 3550, spec)
	want := []string{
		"tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
		"tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.6/32 match ip dport 3550 0xffff flowid 1:4",
	}
	for i := range want {
		if i >= len(got) || strings.Join(got[i], " ") != want[i] {
			t.Fatalf("plan[%d] = %v, want %q", i, got, want[i])
		}
	}
	noPort := netemPlan("eth0", ips[:1], 0, spec)
	if l := strings.Join(noPort[2], " "); strings.Contains(l, "dport") {
		t.Fatalf("a target without a port must not match dport: %s", l)
	}
}

const ipLinkJSON = `[{"ifindex":1,"ifname":"lo","flags":["LOOPBACK","UP","LOWER_UP"],"link_type":"loopback"},` +
	`{"ifindex":2,"ifname":"eth1","flags":["BROADCAST","MULTICAST"],"link_type":"ether"},` +
	`{"ifindex":3,"ifname":"eth0","flags":["BROADCAST","MULTICAST","UP","LOWER_UP"],"link_type":"ether"}]`

func TestPickInterface(t *testing.T) {
	if got := pickInterface([]byte(ipLinkJSON)); got != "eth0" {
		t.Fatalf("pickInterface = %q, want eth0 (up, ether, not loopback)", got)
	}
	if got := pickInterface([]byte(`garbage`)); got != "" {
		t.Fatalf("unparseable listing must yield \"\", got %q", got)
	}
	if got := pickInterface([]byte(`[{"ifname":"lo","flags":["LOOPBACK","UP"],"link_type":"loopback"}]`)); got != "" {
		t.Fatalf("loopback only must yield \"\", got %q", got)
	}
}
```

Append to `internal/agent/cgroup_test.go`:

```go
func TestFirstPID(t *testing.T) {
	dir := cgroupDir(t, map[string]string{"cgroup.procs": "4242\n4243\n"})
	if pid, err := FirstPID(dir); err != nil || pid != 4242 {
		t.Fatalf("FirstPID = %d, %v", pid, err)
	}
	if _, err := FirstPID(cgroupDir(t, map[string]string{"cgroup.procs": "\n"})); err == nil {
		t.Fatal("an empty cgroup must not yield a pid")
	}
	if _, err := FirstPID(t.TempDir()); err == nil {
		t.Fatal("a missing cgroup.procs must be an error")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 -run 'TestNetem|TestPickInterface|TestFirstPID' ./internal/agent/`
Expected: build failure, `undefined: NetemSpecFromRequest` (and the other new names).

- [ ] **Step 3: Implement `internal/agent/netem.go` and `FirstPID` as specified under Interfaces**

File doc comment: netem is applied on the pod's egress interface inside its network namespace; egress only; `scope` semantics; one plan per fault.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 -run 'TestNetem|TestPickInterface|TestFirstPID' ./internal/agent/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/agent`

- [ ] **Step 5: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add internal/agent/netem.go internal/agent/netem_test.go internal/agent/cgroup.go internal/agent/cgroup_test.go
git commit -m "feat(netem): agent netem spec, tc plan, interface and pid discovery

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: agent applies and restores netem with a journal (exec-stubbed)

**Files:**
- Create: `internal/agent/netem_apply.go`
- Test: `internal/agent/netem_apply_test.go`

**Interfaces:**
- Consumes: `NetemSpec`, `netemArgs`, `netemPlan`, `pickInterface`, `FirstPID` (Task 2); `Target` (`internal/agent/resolve.go:13-20`); the atomic-write / read-dir pattern of `Journal` (`internal/agent/quota.go:39-93`).
- Produces:
  - `type Runner interface { Run(ctx context.Context, name string, args ...string) ([]byte, error) }`.
  - `type ExecRunner struct{}` — `exec.CommandContext(ctx, name, args...).CombinedOutput()`; on error returns `fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))` so tc's text reaches the HTTP error.
  - `type RunnerFunc func(ctx context.Context, name string, args ...string) ([]byte, error)` implementing `Runner` (tests in other packages).
  - `type NetemEntry struct { ID string \`json:"id"\`; PodUID string \`json:"pod_uid"\`; Netns string \`json:"netns"\`; NetnsID string \`json:"netns_id"\`; Dev string \`json:"dev"\`; Applied []string \`json:"applied"\`; AppliedAt time.Time \`json:"applied_at"\` }` — `Netns` is `<ProcRoot>/<pid>/ns/net`; `NetnsID` is `os.Readlink(Netns)` at apply time (`net:[4026532412]`); `Applied` holds the tc lines (space-joined argv) in the order they ran.
  - `type NetemJournal struct{ Dir string }` with `Save(e NetemEntry) error`, `Remove(id string) error`, `Load() ([]NetemEntry, error)` — same file layout as `Journal` (`<Dir>/<id>.json`, tmp + rename; missing dir loads as empty). Refactor `quota.go`'s three methods and these onto shared unexported helpers `writeJournalFile(dir, id string, v any) error`, `removeJournalFile(dir, id string) error`, `readJournalFiles(dir string) ([][]byte, error)`; `Journal`'s behaviour is unchanged (existing quota tests stay green).
  - `var ErrNetemBusy = errors.New("agent: a netem fault is already applied in this pod's network namespace")`.
  - `type Netem struct { Runner Runner; Journal NetemJournal; ProcRoot string; LookupIP func(ctx context.Context, host string) ([]net.IPAddr, error); mu sync.Mutex; active map[string]string }` — `ProcRoot` `""` means `/proc`; `LookupIP` nil means `net.DefaultResolver.LookupIPAddr`; `active` maps netns id → fault id.
  - `func (n *Netem) Apply(ctx context.Context, id string, t Target, spec NetemSpec) (*NetemEntry, error)`.
  - `func (n *Netem) Restore(ctx context.Context, e NetemEntry) error`.
  - `func (n *Netem) RestoreAll(ctx context.Context, logger *slog.Logger) int` — boot restore; returns how many entries were restored or discarded.

`Apply`, in this order (the order is the contract):
1. `pid := FirstPID(t.CgroupDir)`; `netns := filepath.Join(procRoot, strconv.Itoa(pid), "ns", "net")`; `netnsID, err := os.Readlink(netns)` (error → fail: the container has no process or the agent lacks hostPID).
2. Under `mu`: if `active[netnsID] != ""` return `ErrNetemBusy`; otherwise reserve `active[netnsID] = id`. Every later failure releases it.
3. Target: `""` → no ips. Otherwise `net.SplitHostPort` (no port → host = target, port 0); `net.ParseIP(host)` literal, else `LookupIP`; keep addresses with `To4() != nil`; none → error `agent: target %q resolved to no IPv4 address`.
4. `dev`: `Runner.Run(ctx, "nsenter", "--net="+netns, "ip", "-json", "link", "show")` → `pickInterface`; on error or `""` use `eth0`.
5. Build `plan := netemPlan(dev, ips, port, spec)`, the entry (`Applied` = joined lines), and `Journal.Save(entry)` **before** the first tc command.
6. Run each line as `Runner.Run(ctx, "nsenter", "--net="+netns, line...)`. On the first error: best-effort `nsenter --net=<netns> tc qdisc del dev <dev> root` (context.Background with 10 s), `Journal.Remove(id)`, release `active`, return the runner's error.
7. Return the entry.

`Restore`: `cur, err := os.Readlink(e.Netns)`; if `err != nil || cur != e.NetnsID` the namespace is gone or reused: remove the journal entry, release `active`, return nil without running tc. Otherwise run `nsenter --net=<e.Netns> tc qdisc del dev <e.Dev> root`; on error return it and **keep** the journal entry (mirrors `RestoreQuota`, `quota.go:152-158`); on success remove the entry and release `active`.

`RestoreAll`: `Load`, `Restore` each, log per entry (`agent: restored journaled netem` / `agent: journaled netem namespace gone` / warn on error), count successes.

- [ ] **Step 1: Write the failing tests**

`internal/agent/netem_apply_test.go`:

```go
package agent

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// recRunner records every command line and answers from substring tables.
type recRunner struct {
	mu    sync.Mutex
	calls []string
	out   map[string][]byte // substring of the line → stdout
	fail  map[string]error  // substring of the line → error (output "RTNETLINK answers: Invalid argument")
	seen  func(line string) // optional probe, called before answering
}

func (r *recRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")
	r.mu.Lock()
	r.calls = append(r.calls, line)
	r.mu.Unlock()
	if r.seen != nil {
		r.seen(line)
	}
	for k, err := range r.fail {
		if strings.Contains(line, k) {
			return []byte("RTNETLINK answers: Invalid argument"), err
		}
	}
	for k, out := range r.out {
		if strings.Contains(line, k) {
			return out, nil
		}
	}
	return nil, nil
}

func (r *recRunner) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recRunner) has(sub string) bool {
	for _, l := range r.lines() {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

// newTestNetem builds a Netem over a fake /proc (pid 4242 in net:[4026532412]),
// a recording runner and a resolver answering 10.43.0.5. The target's
// cgroup.procs names pid 4242.
func newTestNetem(t *testing.T) (*Netem, *recRunner, string, Target) {
	t.Helper()
	proc := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proc, "4242", "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	netns := filepath.Join(proc, "4242", "ns", "net")
	if err := os.Symlink("net:[4026532412]", netns); err != nil {
		t.Fatal(err)
	}
	r := &recRunner{out: map[string][]byte{"ip -json link show": []byte(ipLinkJSON)}, fail: map[string]error{}}
	n := &Netem{Runner: r, Journal: NetemJournal{Dir: filepath.Join(t.TempDir(), "netem")}, ProcRoot: proc,
		LookupIP: func(_ context.Context, host string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.43.0.5")}}, nil
		}}
	tg := Target{PodUID: "pod-a", Container: "server", ContainerID: "c1", CgroupDir: cgroupDir(t, map[string]string{"cgroup.procs": "4242\n"})}
	return n, r, netns, tg
}

var lossSpec = NetemSpec{FaultType: "packet_loss", Target: "productcatalogservice:3550", Loss: 10, Correlation: 25}

func TestNetem_ApplyJournalsBeforeTCAndRestoreRemovesEntry(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	journaledBeforeTC := false
	r.seen = func(line string) {
		if strings.Contains(line, " tc qdisc add ") && !journaledBeforeTC {
			es, _ := n.Journal.Load()
			journaledBeforeTC = len(es) == 1
		}
	}
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	if !journaledBeforeTC {
		t.Fatal("the journal entry must exist before the first tc command runs")
	}
	ns := "--net=" + netns
	want := []string{
		"nsenter " + ns + " ip -json link show",
		"nsenter " + ns + " tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"nsenter " + ns + " tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"nsenter " + ns + " tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
	}
	if got := r.lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if e.Netns != netns || e.NetnsID != "net:[4026532412]" || e.Dev != "eth0" || e.PodUID != "pod-a" || len(e.Applied) != 3 {
		t.Fatalf("entry = %+v", e)
	}
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}
	if last := r.lines()[len(r.lines())-1]; last != "nsenter "+ns+" tc qdisc del dev eth0 root" {
		t.Fatalf("restore ran %q", last)
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after restore")
	}
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
		t.Fatalf("the namespace must be free again after restore: %v", err)
	}
}

func TestNetem_WholePodLatencyUsesTheRootQdisc(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "latency", Delay: 100 * time.Millisecond, Jitter: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if got := r.lines(); len(got) != 2 || got[1] != "nsenter --net="+netns+" tc qdisc add dev eth0 root handle 1: netem delay 100ms 20ms" {
		t.Fatalf("commands = %q", got)
	}
}

func TestNetem_RefusesASecondFaultInTheSameNamespace(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "f1", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	before := len(r.lines())
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); !errors.Is(err, ErrNetemBusy) {
		t.Fatalf("second apply: err = %v, want ErrNetemBusy", err)
	}
	if len(r.lines()) != before {
		t.Fatal("a refused apply must not run any command")
	}
}

func TestNetem_ApplyRollsBackWhenACommandFails(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["tc filter add"] = errors.New("exit status 2")
	_, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err == nil || !strings.Contains(err.Error(), "exit status 2") {
		t.Fatalf("err = %v, want the tc failure", err)
	}
	if !r.has("tc qdisc del dev eth0 root") {
		t.Fatal("the root qdisc added before the failure must be deleted again")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after a rolled-back apply")
	}
	delete(r.fail, "tc filter add")
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
		t.Fatalf("the namespace must be released after a failed apply: %v", err)
	}
}

func TestNetem_RestoreSkipsAChangedNamespace(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(netns)
	if err := os.Symlink("net:[4026539999]", netns); err != nil { // the pid now belongs to another namespace
		t.Fatal(err)
	}
	before := len(r.lines())
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatalf("restore of a vanished namespace must succeed: %v", err)
	}
	if len(r.lines()) != before {
		t.Fatal("restore must not run tc in a namespace that is not the journaled one")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("the stale entry must be dropped")
	}
}

func TestNetem_RestoreAllAtBoot(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "crashed", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	// "Agent restarts": a fresh Netem over the same journal and runner.
	boot := &Netem{Runner: r, Journal: n.Journal, ProcRoot: n.ProcRoot}
	if got := boot.RestoreAll(context.Background(), slog.New(slog.DiscardHandler)); got != 1 {
		t.Fatalf("RestoreAll = %d, want 1", got)
	}
	if !r.has("tc qdisc del dev eth0 root") {
		t.Fatal("boot restore must delete the journaled qdisc")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after boot restore")
	}
}

func TestNetem_FallsBackToEth0WhenLinkListingFails(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["ip -json link show"] = errors.New("exit status 1")
	e, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "packet_loss", Loss: 1})
	if err != nil || e.Dev != "eth0" {
		t.Fatalf("entry=%+v err=%v, want eth0", e, err)
	}
}

func TestNetem_ResolvesTargetToEveryIPv4(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	n.LookupIP = func(_ context.Context, host string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.43.0.5")}, {IP: net.ParseIP("fe80::1")}, {IP: net.ParseIP("10.43.0.6")}}, nil
	}
	e, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "packet_loss", Target: "catalog", Loss: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !r.has("match ip dst 10.43.0.5/32 flowid") || !r.has("match ip dst 10.43.0.6/32 flowid") || r.has("fe80") {
		t.Fatalf("filters = %q, want one per IPv4 address and no IPv6", r.lines())
	}
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}

	// IPv6-only target: rejected before any tc command, namespace released.
	n.LookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("fe80::1")}}, nil
	}
	before := len(r.lines())
	if _, err := n.Apply(context.Background(), "f2", tg, NetemSpec{FaultType: "packet_loss", Target: "v6only", Loss: 1}); err == nil || !strings.Contains(err.Error(), "IPv4") {
		t.Fatalf("an IPv6-only target must be rejected, got %v", err)
	}
	if len(r.lines()) != before {
		t.Fatal("a target that resolves to nothing usable must not run tc")
	}

	// An IP literal never consults the resolver (a nil LookupIP would panic).
	n.LookupIP = nil
	if _, err := n.Apply(context.Background(), "f3", tg, NetemSpec{FaultType: "packet_loss", Target: "10.43.0.9:80", Loss: 1}); err != nil {
		t.Fatal(err)
	}
	if !r.has("match ip dst 10.43.0.9/32 match ip dport 80 0xffff flowid 1:4") {
		t.Fatalf("literal target: filters = %q", r.lines())
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 -run 'TestNetem_' ./internal/agent/`
Expected: build failure, `undefined: Netem` (and `NetemJournal`, `ErrNetemBusy`).

- [ ] **Step 3: Implement `internal/agent/netem_apply.go` per Interfaces, and refactor `quota.go:42-93` onto the shared journal-file helpers**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 -run 'TestNetem_|TestApplyQuota|TestRestoreAll' ./internal/agent/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/agent` (the quota tests prove the refactor kept `Journal` intact).

- [ ] **Step 5: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add internal/agent/netem_apply.go internal/agent/netem_apply_test.go internal/agent/quota.go
git commit -m "feat(netem): agent applies tc netem in the pod netns with a restore journal

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: agent server: netem faults, `GET /v1/capabilities`, binary flags, image

**Files:**
- Modify: `internal/agent/api.go:44-55` (`Status`), append `CapabilitiesResponse`
- Modify: `internal/agent/server.go:21-35` (`Server`), `:67-74` (`Handler`), `:135-265` (`start`)
- Modify: `cmd/atropos-agent/main.go:37-72`
- Modify: `deploy/agent/Dockerfile:10-13`, `deploy/agent/daemonset.yaml:23-29`
- Test: `internal/agent/server_test.go` (`startTestAgent`, lines 32-72, plus new tests)

**Interfaces:**
- Consumes: `Netem`, `NetemEntry`, `ErrNetemBusy`, `RunnerFunc`, `ExecRunner` (Task 3); `NetemSpecFromRequest` (Task 2); `faultparams.NetworkMechanism`, `faultparams.MechanismNetem` (Task 1).
- Produces:
  - `Status.Netns string \`json:"netns,omitempty"\``, `Status.Dev string \`json:"dev,omitempty"\`` (set for netem faults; `Category` is `"network"`, `Mechanism` `"netem"`, `CgroupDir` the resolved container's).
  - `type CapabilitiesResponse struct { Mechanisms []string \`json:"mechanisms"\` }` (api.go).
  - `Server.Netem *Netem` — nil means netem is not offered.
  - `func (s *Server) Mechanisms() []string` — `["burn","quota"]`, plus `"netem"` when `s.Netem != nil`.
  - Route `GET /v1/capabilities` → 200 `CapabilitiesResponse{Mechanisms: s.Mechanisms()}`.
  - `start` request gate (replaces `server.go:150-153`): `netem := cat == "network" && faultparams.NetworkMechanism(req.Fault.FaultType, req.Fault.Params) == faultparams.MechanismNetem`; a request that is neither `resource` nor netem → 400 `the agent executes resource faults and netem network faults only`; netem with `s.Netem == nil` → 400 `netem is not available on this node agent (tc or nsenter missing)`. The `duration_ms > 0`, own-pod and resolver checks apply to both. `faultparams.ValidateMechanism` (resource mechanisms) runs only for the resource branch.
  - netem branch: `spec, err := NetemSpecFromRequest(req.Fault)` → 400; `entry, err := s.Netem.Apply(r.Context(), id, t, spec)` → 409 when `errors.Is(err, ErrNetemBusy)`, else 500 with the error text; then `stop, done = startTimed(duration, map[string]any{"netem": entry}, release)` where `release` restores with `context.WithTimeout(context.Background(), 15*time.Second)` (the request context is already cancelled on disconnect) and logs failures.
  - `func startTimed(duration time.Duration, detail map[string]any, release func() error) (stop func(), done <-chan Result)` — extracted from the quota branch (`server.go:195-213`): `release` runs exactly once, when `duration` elapses or `stop` is called; the result carries `ActualDurationMs`, `Detail` = `detail` plus `"ended": "duration"|"stopped"`, and `Error` from `release`. The quota branch is rewritten onto it with `detail = {"quota": q}` and `release = RestoreQuota(...)` (logged on error, as today).
  - `cmd/atropos-agent/main.go`: flag `-netem-journal` (default `/var/lib/atropos-agent/netem`); `nt := &agent.Netem{Runner: agent.ExecRunner{}, Journal: agent.NetemJournal{Dir: *netemJournal}, ProcRoot: *procRoot}`; if `exec.LookPath("tc")` or `exec.LookPath("nsenter")` fails, log a warning naming the missing binary and set `nt = nil`; otherwise `nt.RestoreAll(ctx, logger)` before serving (warn with the count when > 0, like quotas at `main.go:52-54`); `srv.Netem = nt`; the ready log line adds `"mechanisms", srv.Mechanisms()`.
  - `deploy/agent/Dockerfile`: after `FROM alpine:3.22` add `RUN apk add --no-cache iproute2 util-linux && tc -V && nsenter -V` (the build fails early if either binary is missing). `deploy/agent/daemonset.yaml:29`: add `- -netem-journal=/var/lib/atropos-agent/netem` (same `journal` hostPath volume).

- [ ] **Step 1: Write the failing tests**

In `internal/agent/server_test.go`: extend `testAgent` with `runner *recRunner` and `netns string`; in `startTestAgent` build `nt, rec, netns, _ := newTestNetem(t)` and set `srv.Netem = nt` (the target cgroup stays the existing `dir`; the Netem's own target from `newTestNetem` is unused here). Add helpers and tests:

```go
// seedPID makes the target cgroup name pid 4242, the pid newTestNetem's fake /proc knows.
func (a *testAgent) seedPID(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(a.dir, "cgroup.procs"), []byte("4242\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func netemReq(pod, faultType, target, direction, params string, durationMs int64) StartRequest {
	return StartRequest{PodUID: pod, Container: "server", Name: "netem-" + faultType, Fault: wire.FaultRequest{
		Category: "network", FaultType: faultType, DurationMs: durationMs,
		Network: &wire.NetworkEnvelope{Host: "inline", Target: target, Direction: direction},
		Params:  json.RawMessage(params)}}
}

func TestServer_CapabilitiesEndpoint(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	get := func() string {
		r, _ := http.NewRequest(http.MethodGet, "http://agent/v1/capabilities", nil)
		resp, err := a.client.Do(r)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("GET /v1/capabilities: %v %v", err, resp)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return strings.TrimSpace(string(b))
	}
	if got := get(); got != `{"mechanisms":["burn","quota","netem"]}` {
		t.Fatalf("capabilities = %s", got)
	}
	a.srv.Netem = nil
	if got := get(); got != `{"mechanisms":["burn","quota"]}` {
		t.Fatalf("capabilities without netem = %s", got)
	}
}

func TestServer_NetemAppliesTargetedPlanAndRestoresOnDelete(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, id := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "productcatalogservice:3550", "upstream", `{"loss":10,"correlation":25}`, 10000))
	defer resp.Body.Close()
	if id == "" {
		t.Fatal("no id")
	}
	ns := "--net=" + a.netns
	want := []string{
		"nsenter " + ns + " ip -json link show",
		"nsenter " + ns + " tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"nsenter " + ns + " tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"nsenter " + ns + " tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
	}
	if got := a.runner.lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 1 || es[0].ID != id || es[0].PodUID != "pod-a" {
		t.Fatalf("journal = %+v", es)
	}
	lr, _ := http.NewRequest(http.MethodGet, "http://agent/v1/faults", nil)
	lresp, _ := a.client.Do(lr)
	var st []Status
	json.NewDecoder(lresp.Body).Decode(&st)
	lresp.Body.Close()
	if len(st) != 1 || st[0].ID != id || st[0].Category != "network" || st[0].FaultType != "packet_loss" || st[0].Mechanism != "netem" || st[0].Dev != "eth0" || st[0].Netns != a.netns {
		t.Fatalf("list = %+v", st)
	}
	dr, _ := http.NewRequest(http.MethodDelete, "http://agent/v1/faults/"+id, nil)
	dresp, err := a.client.Do(dr)
	if err != nil || dresp.StatusCode != 200 {
		t.Fatalf("delete: %v %v", err, dresp)
	}
	dresp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line after stop")
	}
	var res Result
	json.Unmarshal(sc.Bytes(), &res)
	if res.Error != "" {
		t.Fatalf("result = %+v", res)
	}
	if l := a.runner.lines(); l[len(l)-1] != "nsenter "+ns+" tc qdisc del dev eth0 root" {
		t.Fatalf("last command = %q, want the root qdisc deleted", l[len(l)-1])
	}
	if es, _ := a.srv.Netem.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after restore")
	}
}

func TestServer_NetemLatencyWholePodExpiresByDuration(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, _ := a.post(t, context.Background(), netemReq("pod-a", "latency", "", "", `{"delay":"100ms","jitter":"20ms","mechanism":"netem"}`, 200))
	defer resp.Body.Close()
	if !a.runner.has("tc qdisc add dev eth0 root handle 1: netem delay 100ms 20ms") {
		t.Fatalf("commands = %q", a.runner.lines())
	}
	sc := bufio.NewScanner(resp.Body)
	if !sc.Scan() {
		t.Fatal("no result line")
	}
	if !a.runner.has("tc qdisc del dev eth0 root") {
		t.Fatal("expiry must delete the qdisc")
	}
}

func TestServer_NetemRestoresWhenTheSidecarDisconnects(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	ctx, cancel := context.WithCancel(context.Background())
	resp, _ := a.post(t, ctx, netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000))
	cancel() // the sidecar is gone
	resp.Body.Close()
	deadline := time.Now().Add(5 * time.Second)
	for !a.runner.has("tc qdisc del dev eth0 root") && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !a.runner.has("tc qdisc del dev eth0 root") {
		t.Fatal("netem must be restored when the caller disappears")
	}
}

func TestServer_NetemRefusesASecondFaultInTheSameNamespace(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	resp, _ := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 10000))
	defer resp.Body.Close()
	before := len(a.runner.lines())
	resp2, body := a.post(t, context.Background(), netemReq("pod-a", "latency", "", "", `{"delay":"1ms","mechanism":"netem"}`, 10000))
	if resp2.StatusCode != http.StatusConflict || !strings.Contains(body, "already applied") {
		t.Fatalf("second netem: status=%d body=%q", resp2.StatusCode, body)
	}
	if len(a.runner.lines()) != before {
		t.Fatal("a refused request must not touch the namespace")
	}
}

func TestServer_NetemRejectsBadRequests(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	a.seedPID(t)
	cases := []struct {
		name string
		req  StartRequest
		code int
	}{
		{"latency without mechanism is a toxic, not an agent fault", netemReq("pod-a", "latency", "", "", `{"delay":"1ms"}`, 1000), http.StatusBadRequest},
		{"downstream is ingress", netemReq("pod-a", "packet_loss", "x", "downstream", `{"loss":10}`, 1000), http.StatusBadRequest},
		{"duration required", netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 0), http.StatusBadRequest},
		{"another pod", netemReq("pod-b", "packet_loss", "", "", `{"loss":10}`, 1000), http.StatusForbidden},
	}
	for _, c := range cases {
		resp, body := a.post(t, context.Background(), c.req)
		if resp.StatusCode != c.code {
			t.Errorf("%s: status=%d body=%q, want %d", c.name, resp.StatusCode, body, c.code)
		}
	}
	if len(a.runner.lines()) != 0 {
		t.Fatalf("rejected requests must run nothing, ran %q", a.runner.lines())
	}
	a.srv.Netem = nil
	if resp, body := a.post(t, context.Background(), netemReq("pod-a", "packet_loss", "", "", `{"loss":10}`, 1000)); resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "not available") {
		t.Fatalf("netem without tc: status=%d body=%q", resp.StatusCode, body)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 -run 'TestServer_' ./internal/agent/`
Expected: build failure, `a.srv.Netem undefined` / `a.runner undefined`.

- [ ] **Step 3: Implement the server changes (`Server.Netem`, `Mechanisms`, the capabilities route, `startTimed`, the netem branch of `start`, the new `Status` fields) and update `startTestAgent`**

Keep the quota tests green while swapping the quota branch onto `startTimed`; the log line at `server.go:240` gains `"dev"`/`"netns"` for netem.

- [ ] **Step 4: Run the agent tests to verify they pass**

Run: `go test -count=1 ./internal/agent/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/agent` (re-run once if only `TestStartBurn_StopEndsTheChildEarly` fails: the known darwin flake).

- [ ] **Step 5: Wire `cmd/atropos-agent/main.go`, the Dockerfile and the DaemonSet as specified under Interfaces**

Run: `GOOS=linux go build ./cmd/... && go run ./cmd/atropos-agent -h 2>&1 | grep -c netem-journal`
Expected: `1`.

- [ ] **Step 6: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add internal/agent/api.go internal/agent/server.go internal/agent/server_test.go cmd/atropos-agent/main.go deploy/agent/Dockerfile deploy/agent/daemonset.yaml
git commit -m "feat(netem): agent serves netem faults and GET /v1/capabilities; image gains iproute2 and util-linux

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `instrument`: `WithNetworkHost` decode hook and the `netem` capability

**Files:**
- Modify: `internal/instrument/capabilities.go:7-12`
- Modify: `internal/instrument/decode.go:26-43` (config + hook), `:216-224` (network case of `decodeFault`)
- Test: `internal/instrument/capabilities_test.go` (append)

**Interfaces:**
- Consumes: `faultparams.NetworkMechanism`, `faultparams.MechanismNetem` (Task 1).
- Produces:
  - `const CapNetem = "netem"` with the comment that it is a mechanism capability, advertised alongside the category capabilities, that only the sidecar with a netem-capable node agent lists; `AllCapabilities()` (`capabilities.go:22-24`) is **unchanged** (the in-process host never executes netem).
  - `type NetworkHost func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error)` and `func WithNetworkHost(h NetworkHost) DecodeOption` storing `decodeConfig.networkHost`; doc: the host that executes a network fault whose mechanism is `netem` (the sidecar, delegating to the node agent); every other network fault keeps the proxy/inline toxic path.
  - In `decodeFault`'s `"network"` case, after the nil-envelope check and before `decodeNetworkFault`: when `faultparams.NetworkMechanism(f.FaultType, params) == faultparams.MechanismNetem`, return `&UnsupportedFaultError{Category: "network", FaultType: f.FaultType + "/netem"}` if `cfg.networkHost == nil` **or** `cfg.capabilities != nil && !cfg.capabilities[CapNetem]`; otherwise `return cfg.networkHost(name, f, baseCfg)`. (`UnsupportedFaultError` is skipped per rule and counted under `network`, `decode.go:102-110`.)

- [ ] **Step 1: Write the failing tests**

Append to `internal/instrument/capabilities_test.go`:

```go
func netemRule(name, faultType, params string) wire.CompiledRule {
	return wire.CompiledRule{Name: name, InjectionPoint: "egress", Mode: "background",
		Fault: &wire.CompiledFault{Category: "network", FaultType: faultType, DurationMs: 5000,
			Network: &wire.NetworkEnvelope{Host: "inline", Target: "productcatalogservice:3550", Direction: "upstream"},
			Params:  json.RawMessage(params)}}
}

func TestDecode_NetemRoutesToTheNetworkHost(t *testing.T) {
	var got []string
	stub := &inline.Latency{FaultConfig: fault.FaultConfig{Duration: time.Second}, Delay: time.Millisecond}
	host := func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		got = append(got, name+"/"+req.FaultType+"/"+req.Network.Target)
		if base.Duration != 5*time.Second {
			t.Errorf("base duration = %v", base.Duration)
		}
		return stub, nil
	}
	rules, err := decodeCompiledRules([]wire.CompiledRule{
		netemRule("lat-netem", "latency", `{"delay":"100ms","mechanism":"netem"}`),
		netemRule("loss", "packet_loss", `{"loss":10}`),
		netemRule("lat-toxic", "latency", `{"delay":"100ms"}`),
	}, WithNetworkHost(host), withNetworkResolver(stubResolver(":19090", "localhost:3550")))
	if err != nil || len(rules) != 3 {
		t.Fatalf("rules=%d err=%v", len(rules), err)
	}
	if strings.Join(got, ",") != "lat-netem/latency/productcatalogservice:3550,loss/packet_loss/productcatalogservice:3550" {
		t.Fatalf("hook calls = %v", got)
	}
	if rules[0].Decision.Fault != stub || rules[1].Decision.Fault != stub {
		t.Fatal("the hook's fault must be what the netem rules carry")
	}
	if _, ok := rules[2].Decision.Fault.(*network.Proxy); !ok {
		t.Fatalf("latency without mechanism must stay a toxic proxy fault, got %T", rules[2].Decision.Fault)
	}
}

func TestDecode_NetemWithoutANetworkHostIsSkippedAndCounted(t *testing.T) {
	before := unsupportedCount(t, "network")
	rules, err := decodeCompiledRules([]wire.CompiledRule{netemRule("loss", "packet_loss", `{"loss":10}`)},
		withNetworkResolver(stubResolver(":19090", "localhost:3550")))
	if err != nil || len(rules) != 0 || unsupportedCount(t, "network")-before != 1 {
		t.Fatalf("rules=%d err=%v counted=%v (netem needs the node agent; never a sync failure)", len(rules), err, unsupportedCount(t, "network")-before)
	}
}

func TestDecode_NetemNeedsTheNetemCapability(t *testing.T) {
	host := func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		return &inline.Latency{FaultConfig: base, Delay: time.Millisecond}, nil
	}
	rule := []wire.CompiledRule{netemRule("loss", "packet_loss", `{"loss":10}`)}
	before := unsupportedCount(t, "network")
	rules, err := decodeCompiledRules(rule, WithNetworkHost(host), WithCapabilities(CapInline, CapNetwork, CapCacheBox))
	if err != nil || len(rules) != 0 || unsupportedCount(t, "network")-before != 1 {
		t.Fatalf("without CapNetem: rules=%d err=%v", len(rules), err)
	}
	rules, err = decodeCompiledRules(rule, WithNetworkHost(host), WithCapabilities(CapInline, CapNetwork, CapCacheBox, CapNetem))
	if err != nil || len(rules) != 1 {
		t.Fatalf("with CapNetem: rules=%d err=%v", len(rules), err)
	}
	for _, c := range AllCapabilities() {
		if c == CapNetem {
			t.Fatal("the in-process host must never advertise netem")
		}
	}
}
```

(`network` is already imported by `decode_test.go` in the same package; `capabilities_test.go` needs `"strings"` and `"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"` added to its imports.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 -run 'TestDecode_Netem' ./internal/instrument/`
Expected: build failure, `undefined: WithNetworkHost`, `undefined: CapNetem`.

- [ ] **Step 3: Implement `CapNetem`, `NetworkHost`, `WithNetworkHost` and the routing in `decodeFault`**

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/instrument/`
Expected: `ok  	git.ucsc.edu/microfaults/atropos-go/internal/instrument`

- [ ] **Step 5: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add internal/instrument/capabilities.go internal/instrument/capabilities_test.go internal/instrument/decode.go
git commit -m "feat(netem): WithNetworkHost decode hook and the netem capability

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: sidecar: agent capability probe, `NetworkFactory`, `atroposd` advertises `netem`

**Files:**
- Modify: `internal/agent/client.go:134-153` (append `Capabilities`, `NetworkFactory`)
- Modify: `internal/atroposd/daemon.go:83-95`
- Test: `internal/agent/client_test.go`, `internal/atroposd/daemon_test.go`

**Interfaces:**
- Consumes: `CapabilitiesResponse`, `GET /v1/capabilities` (Task 4); `instrument.NetworkHost`, `instrument.WithNetworkHost`, `instrument.CapNetem` (Task 5); `NetemSpecFromRequest` (Task 2); `AgentFault`, `socketClient` (`client.go:24-53`).
- Produces:
  - `func Capabilities(sock string) (CapabilitiesResponse, error)` — `GET /v1/capabilities` with a 2 s timeout; 200 → decoded body; **404 → `CapabilitiesResponse{Mechanisms: []string{faultparams.MechanismBurn, faultparams.MechanismQuota}}` and nil error** (an agent predating the endpoint); dial failure or any other status → error. `Reachable` (`client.go:143-153`) stays as is.
  - `func NetworkFactory(sock, podUID, container string) instrument.NetworkHost` — runs `NetemSpecFromRequest(*req)` first so a malformed netem rule fails at decode with the agent's own message (a decode error, not an `UnsupportedFaultError`: it is a bad rule, like an unparseable delay), then returns `&AgentFault{FaultConfig: base, Sock: sock, PodUID: podUID, Container: container, Name: name, Req: *req}`.
  - `daemon.go` agent block becomes: `if cfg.AgentSocket != "" { if ac, err := agent.Capabilities(cfg.AgentSocket); err == nil { caps += CapResource; decodeOpts += WithResourceHost(ResourceFactory(...)); if slices.Contains(ac.Mechanisms, faultparams.MechanismNetem) { caps += CapNetem; decodeOpts += WithNetworkHost(NetworkFactory(sock, podUID, container)) }; log "atroposd: node agent reachable; resource faults delegated" with `"mechanisms", ac.Mechanisms` } }` (one probe replaces `agent.Reachable` here; an unreachable agent is still silently not advertised, as today).

- [ ] **Step 1: Write the failing tests**

Append to `internal/agent/client_test.go`:

```go
func TestCapabilities_ReportsTheAgentsMechanisms(t *testing.T) {
	a := startTestAgent(t, "pod-a")
	ac, err := Capabilities(a.sock)
	if err != nil || strings.Join(ac.Mechanisms, ",") != "burn,quota,netem" {
		t.Fatalf("capabilities = %v, %v", ac, err)
	}
	if _, err := Capabilities(shortSocket(t)); err == nil {
		t.Fatal("a missing socket must be an error")
	}
}

func TestCapabilities_LegacyAgentReportsBurnAndQuota(t *testing.T) {
	sock := shortSocket(t)
	ln, err := Listen(sock)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/faults", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("[]")) })
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	ac, err := Capabilities(sock)
	if err != nil || strings.Join(ac.Mechanisms, ",") != "burn,quota" {
		t.Fatalf("legacy agent: %v, %v (a 404 means burn and quota)", ac, err)
	}
}

func TestNetworkFactory_ValidatesAtDecodeAndBuildsAnAgentFault(t *testing.T) {
	h := NetworkFactory("/tmp/x.sock", "pod-a", "server")
	req := netReq("packet_loss", "productcatalogservice:3550", "upstream", 0, `{"loss":10}`)
	f, err := h("drop-catalog", &req, fault.FaultConfig{Duration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	af, ok := f.(*AgentFault)
	if !ok || af.Name != "drop-catalog" || af.PodUID != "pod-a" || af.Container != "server" || af.Req.FaultType != "packet_loss" || af.Req.Network.Target != "productcatalogservice:3550" {
		t.Fatalf("fault = %#v", f)
	}
	bad := netReq("latency", "", "", 0, `{"delay":"1ms"}`)
	if _, err := h("toxic", &bad, fault.FaultConfig{Duration: time.Second}); err == nil || !strings.Contains(err.Error(), "toxic") {
		t.Fatalf("a non-netem latency must fail decode through the factory, got %v", err)
	}
}
```

(`client_test.go` gains imports `net/http`, `strings`; `netReq` comes from `netem_test.go`, Task 2.)

In `internal/atroposd/daemon_test.go`: extract the agent setup of `TestRun_DelegatesResourceFaultsToAgent` (lines 343-373) into `startFakeAgent(t *testing.T, cg string, nt *agent.Netem) (sock string, srv *agent.Server)`, call it from that test with `nil`, and add:

```go
// fakeManteion answers the init/register/rules/events/deregister calls Run
// makes and hands every register body to registered.
func fakeManteion(t *testing.T, registered chan<- []byte) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/sdk/init", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("POST /api/v1/sdk/register", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		select {
		case registered <- b:
		default:
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"status":"registered","version":1,"rules":[],"active_faults":[]}`))
	})
	mux.HandleFunc("DELETE /api/v1/sdk/register/{id}", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /api/v1/sdk/rules", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"version":1,"rules":[],"active_faults":[]}`))
	})
	mux.HandleFunc("GET /api/v1/sdk/events", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no sse", http.StatusNotFound) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// podNetem is a Netem over a fake /proc whose pid 4242 lives in one namespace;
// cg's cgroup.procs names that pid.
func podNetem(t *testing.T, cg string, rec *[]string, mu *sync.Mutex) *agent.Netem {
	t.Helper()
	proc := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proc, "4242", "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("net:[4026532412]", filepath.Join(proc, "4242", "ns", "net")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cg, "cgroup.procs"), []byte("4242\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return &agent.Netem{
		Runner: agent.RunnerFunc(func(_ context.Context, name string, args ...string) ([]byte, error) {
			mu.Lock()
			*rec = append(*rec, strings.Join(append([]string{name}, args...), " "))
			mu.Unlock()
			return nil, nil // ip -json link show yields nothing → eth0
		}),
		Journal:  agent.NetemJournal{Dir: filepath.Join(t.TempDir(), "netem")},
		ProcRoot: proc,
		LookupIP: func(context.Context, string) ([]net.IPAddr, error) { return []net.IPAddr{{IP: net.ParseIP("10.43.0.5")}}, nil },
	}
}

func TestRun_NetemThroughTheAgent(t *testing.T) {
	cg := t.TempDir()
	var rec []string
	var mu sync.Mutex
	sock, _ := startFakeAgent(t, cg, podNetem(t, cg, &rec, &mu))
	has := func(sub string) bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range rec {
			if strings.Contains(l, sub) {
				return true
			}
		}
		return false
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(up.Close)
	addrs := runDaemon(t, Config{
		Service: "recommendationservice", Egress: []EgressRoute{{Listen: "127.0.0.1:0", Upstream: strings.TrimPrefix(up.URL, "http://")}},
		AgentSocket: sock, PodUID: "pod-x", AppContainer: "server",
	})
	control := "http://" + addrs.Control
	rule := `[{"name":"drop-catalog","injection_point":"egress","mode":"background",
		"fault":{"category":"network","fault_type":"packet_loss","duration_ms":10000,
		"network":{"target":"productcatalogservice:3550","direction":"upstream"},"params":{"loss":10,"correlation":25}}}]`
	if code, body := postJSON(t, control+"/admin/rules", rule); code != http.StatusNoContent {
		t.Fatalf("install rule: code=%d body=%s", code, body)
	}
	want := "tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4"
	deadline := time.Now().Add(5 * time.Second)
	for !has(want) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !has(want) {
		t.Fatalf("eager arming must apply netem through the agent, ran %q", rec)
	}
	if code, body := postJSON(t, control+"/admin/rules", `[]`); code != http.StatusNoContent {
		t.Fatalf("clear rules: code=%d body=%s", code, body)
	}
	deadline = time.Now().Add(5 * time.Second)
	for !has("tc qdisc del dev eth0 root") && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if !has("tc qdisc del dev eth0 root") {
		t.Fatal("rule removal must restore the pod's qdisc")
	}
}

func TestRun_AdvertisesNetemOnlyWhenTheAgentOffersIt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		netem    bool
		wantCaps string
	}{
		{"agent with netem", true, `"capabilities":["inline","network","cachebox","resource","netem"]`},
		{"agent without netem", false, `"capabilities":["inline","network","cachebox","resource"]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cg := t.TempDir()
			var rec []string
			var mu sync.Mutex
			var nt *agent.Netem
			if tc.netem {
				nt = podNetem(t, cg, &rec, &mu)
			}
			sock, _ := startFakeAgent(t, cg, nt)
			registered := make(chan []byte, 1)
			m := fakeManteion(t, registered)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
			t.Cleanup(up.Close)
			runDaemon(t, Config{
				Service: "recommendationservice", ManteionURL: m.URL,
				Egress:      []EgressRoute{{Listen: "127.0.0.1:0", Upstream: strings.TrimPrefix(up.URL, "http://")}},
				AgentSocket: sock, PodUID: "pod-x", AppContainer: "server",
			})
			select {
			case body := <-registered:
				if !strings.Contains(string(body), tc.wantCaps) {
					t.Fatalf("register body = %s, want %s", body, tc.wantCaps)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("no registration")
			}
		})
	}
}
```

(`daemon_test.go` gains the import `sync`; `startFakeAgent` writes the same `cpu.max`/`memory.max`/`memory.high`/`cgroup.procs` files the extracted code wrote, builds the `agent.Server` with `Netem: nt`, listens, serves until cleanup, and returns the socket path.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test -count=1 -run 'TestCapabilities_|TestNetworkFactory_' ./internal/agent/ && go test -count=1 -run 'TestRun_Netem|TestRun_Advertises' ./internal/atroposd/`
Expected: build failures, `undefined: Capabilities` / `undefined: NetworkFactory`; then, once those exist, `TestRun_AdvertisesNetemOnlyWhenTheAgentOffersIt/agent_with_netem` fails on the missing `"netem"` capability and `TestRun_NetemThroughTheAgent` times out waiting for the filter line (the daemon still skips netem rules).

- [ ] **Step 3: Implement `Capabilities`, `NetworkFactory` and the `daemon.go` wiring as specified under Interfaces**

`daemon.go` imports `faultparams` and `slices`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -count=1 ./internal/agent/ ./internal/atroposd/`
Expected: both `ok`.

- [ ] **Step 5: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok`.

```bash
git add internal/agent/client.go internal/agent/client_test.go internal/atroposd/daemon.go internal/atroposd/daemon_test.go
git commit -m "feat(netem): atroposd advertises netem and routes netem faults to the agent when it offers them

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: docs: README, agent README with the k3s verification, AGENTS.md sidecar note, design status

**Files:**
- Modify: `README.md:51` (feature row), `README.md:344-364` (sidecar resource-faults paragraph and the "Not in this release" list)
- Modify: `deploy/agent/README.md:12-23` (mechanisms), `:38-45` (node checks), append "Verify netem on a k3s node"
- Modify: `AGENTS.md:144-154` (atroposd section)
- Modify: `docs/plans/2026-09-08-atropos-daemon-design.md:619-622` (status paragraph), `docs/plans/2026-09-27-daemon-design-review-2.md:296` (netem row disposition)

- [ ] **Step 1: README**

Feature table (`README.md:51`): extend the node-agent row with `netem` on the pod's egress (`packet_loss`, kernel-level `latency`) scoped to a dependency by ClusterIP, journaled and restored like quotas. After the resource-faults paragraph (`README.md:344-360`) add one paragraph: with an agent that reports `netem` on `GET /v1/capabilities`, the sidecar advertises `netem` and routes `network/packet_loss` and `network/latency` with `mechanism: netem` to the agent, which applies `tc netem` inside the pod's network namespace on its egress interface (`prio` root + `u32` filter on the target's ClusterIP(s) and port, or on the root when no target is given, which then also shapes the sidecar's own control-plane and OTel traffic); `scope` multiplies `loss`; egress only (`direction: upstream`); one netem fault per pod at a time; the in-process host and a sidecar without the agent skip such rules and count them. Add `IPv6 targets` and `netem ingress shaping (ifb)` to the "Not in this release" line (`README.md:361-364`).

- [ ] **Step 2: `deploy/agent/README.md`**

Add a third mechanism bullet (`:12-23`) for `netem`: what it applies, the journal at `/var/lib/atropos-agent/netem`, restore on stop / disconnect / boot, and that the agent only reports `netem` when `tc` and `nsenter` are on its PATH. Extend the node checks (`:38-45`) with `modinfo sch_netem sch_prio cls_u32 | grep -c ^filename   # 3`. Append this section verbatim:

````markdown
## Verify netem on a k3s node

Unit tests stub `nsenter`/`tc`; this is the real check, on VM1 after the
agent DaemonSet and a sidecar-instrumented service are up.

```bash
POD=$(sudo k3s kubectl get pod -l app=recommendationservice -o jsonpath='{.items[0].metadata.name}')
PODIP=$(sudo k3s kubectl get pod $POD -o jsonpath='{.status.podIP}')
CID=$(sudo k3s crictl ps --name server -q | head -1)
PID=$(sudo k3s crictl inspect $CID | jq .info.pid)
CATALOG=$(sudo k3s kubectl get svc productcatalogservice -o jsonpath='{.spec.clusterIP}')
NS="sudo nsenter --net=/proc/$PID/ns/net"

$NS tc qdisc show dev eth0                          # before: "qdisc noqueue 0: root"

# 1. Targeted loss (30% toward productcatalogservice:3550) through the sidecar's admin slot.
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"packet_loss","duration_ms":120000,
  "network":{"target":"productcatalogservice:3550","direction":"upstream"},
  "params":{"loss":30,"correlation":25}}'
$NS tc -s qdisc show dev eth0                       # prio 1: root, netem 40: parent 1:4 "loss 30% 25%"; its "dropped" counter rises under load
$NS tc filter show dev eth0                         # u32 match <CATALOG>/32 and dport 3550, flowid 1:4
sudo ls /var/lib/atropos-agent/netem                # one <id>.json
curl -s -XDELETE http://$PODIP:16020/admin/fault
$NS tc qdisc show dev eth0                          # back to noqueue; journal dir empty

# 2. Loss visible to ping: ICMP has no dport, so target the service without a port.
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"packet_loss","duration_ms":120000,
  "network":{"target":"productcatalogservice","direction":"upstream"},"params":{"loss":30}}'
sudo k3s kubectl exec $POD -c server -- ping -c 50 -i 0.2 -W 1 $CATALOG | tail -2   # ~30% packet loss
curl -s -XDELETE http://$PODIP:16020/admin/fault

# 3. Kernel latency, whole pod.
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"latency","duration_ms":60000,
  "network":{"direction":"upstream"},"params":{"delay":"200ms","jitter":"50ms","mechanism":"netem"}}'
sudo k3s kubectl exec $POD -c server -- ping -c 5 $CATALOG | tail -1               # rtt avg ≈ 200 ms (± jitter)
curl -s -XDELETE http://$PODIP:16020/admin/fault

# 4. Crash restore: arm 1 again, then kill the agent.
sudo k3s kubectl delete pod -l app=atropos-agent
sudo k3s kubectl logs -l app=atropos-agent | grep "restored journaled netem"       # one line
$NS tc qdisc show dev eth0                          # noqueue
```

Transparent capture: the app's connections are REDIRECTed to the sidecar on
loopback and the sidecar dials the ClusterIP from the same namespace, so the
targeted filter shapes the sidecar→dependency leg; the app observes slow or
failing responses from the sidecar. The loopback hop is never shaped.
````

- [ ] **Step 3: `AGENTS.md` sidecar section and the design/review status lines**

`AGENTS.md:150-154`: add "when the node agent reports `netem` (`GET /v1/capabilities`), the sidecar advertises the `netem` capability and routes `network/packet_loss` and netem `latency` to the agent (`internal/agent.NetworkFactory` via `instrument.WithNetworkHost`)". Design doc status paragraph (`:619-622`): append "netem done (plan `2026-10-30-agent-netem.md`): the agent applies `tc netem` in the pod's network namespace on egress, `prio`+`u32` scoping by ClusterIP, journal restored on stop/disconnect/boot; `GET /v1/capabilities` lets the sidecar advertise `netem` precisely." Review-2 netem row (`:296`): prefix the disposition with "Done (`2026-10-30-agent-netem.md`); egress only, ifb for ingress deferred."

- [ ] **Step 4: Gate and commit**

Run: `gofmt -l . && go vet ./... && go test -count=1 ./... && GOOS=linux go build ./cmd/...`
Expected: no gofmt output; every package `ok` (docs only, but the gate is the rule).

```bash
git add README.md deploy/agent/README.md AGENTS.md docs/plans/2026-09-08-atropos-daemon-design.md docs/plans/2026-09-27-daemon-design-review-2.md
git commit -m "docs(netem): README, agent README with k3s verification, sidecar note, design status

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage.** (1) `packet_loss` params + validation, `mechanism: netem` on network latency, AGENTS catalogue → Task 1. (2) pid → netns, `nsenter`/`tc`, image packages → Tasks 2-4; `prio`+`u32` layout scoped by `network.target` with ClusterIP resolution from the agent → Tasks 2-3; `scope` mapping (multiplies loss; unsupported on latency) → Task 2; no target → root netem → Task 2; egress only → Task 2 (`direction` rule) and docs; journal `{id, pod_uid, netns, dev, applied}` (+ `netns_id`, the safety check the spec's silence would otherwise leave to a reused pid) restored on stop / disconnect / boot → Tasks 3-4; interface discovery via `ip -json link`, default `eth0` → Tasks 2-3. (3) `StartRequest.Fault` network + netem accepted; `GET /v1/capabilities` → Task 4. (4) sidecar advertises `netem` only when reported; `WithNetworkHost` → `agent.NetworkFactory`; toxic path untouched; in-process host rejects with `UnsupportedFaultError` → Tasks 5-6. (5) exec stub, journal round-trip, server start/stop via `startTestAgent`, decode routing, documented k3s verification → Tasks 3-7.

**Step scan.** Every code step names its file, signatures and the fixed values; bodies are given only where the signature does not determine them (the `Apply`/`Restore` ordering, the qdisc layout, the request gate). Every test step is runnable code with the exact commands expected.

**Type consistency.** `faultparams.NetworkMechanism(faultType string, raw json.RawMessage) string` is used identically in Task 4 (`server.start`) and Task 5 (`decodeFault`). `NetemSpecFromRequest(req wire.FaultRequest)` is called with a value in Task 4 (`req.Fault`) and Task 6 (`*req`). `Netem.LookupIP` has the `net.DefaultResolver.LookupIPAddr` signature everywhere. `instrument.NetworkHost` (Task 5) is the return type of `agent.NetworkFactory` (Task 6) and the parameter of `WithNetworkHost`. `startTestAgent` fields `runner`/`netns` (Task 4) are what the Task 4 server tests read; `newTestNetem` (Task 3) returns `(*Netem, *recRunner, string, Target)` and Task 4 discards the `Target`. `RunnerFunc` (Task 3) is what `daemon_test.go` (Task 6) uses.

**Review Focus.** Each of the five lines names its test and task; all five tests are written in those tasks.

**Proportion.** The code blocks are tests and argv goldens; implementation is signatures plus the three algorithms the signatures do not fix. The agent's tc semantics are the risky part of this feature, so the goldens are deliberately exact.
