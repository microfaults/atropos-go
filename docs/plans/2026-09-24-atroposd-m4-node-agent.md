# atroposd M4: Node Agent (`atropos-agent`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resource faults for sidecar-instrumented pods, executed in the *target container's* cgroup by a privileged per-node agent that the sidecar delegates to over a host socket. Two mechanisms: `burn` (the existing stress code as a child process placed in the target cgroup; unchanged semantics) and `quota` (lowering the target cgroup's `cpu.max` / `memory.high` / `io.max`, restored on stop, on agent restart, and on lost heartbeat). Design §5.14 (M4 row of §6).

**Architecture:** The sidecar decodes a resource rule through a `WithResourceHost` decode hook (like the inline-toxic hook) into an `AgentFault`, which on `Start` POSTs `{pod_uid, container, fault}` to `/var/run/atropos/agent.sock`, heartbeats while running, and DELETEs on stop; the result streams back on the response so the sidecar's fault span records it. The agent authenticates the caller by `SO_PEERCRED` → `/proc/<pid>/cgroup` → pod UID and refuses any request for another pod. It resolves the target container by reading containerd's OCI runtime specs (`<task root>/k8s.io/<id>/config.json`: annotations `io.kubernetes.cri.sandbox-uid` and `io.kubernetes.cri.container-name`) and locates the container's cgroup directory by scanning the host cgroup v2 hierarchy for `*<id>*`. No CRI gRPC client, no new module dependencies.

**Tech Stack:** Go 1.25; `golang.org/x/sys/unix` (peer credentials); cgroup v2 files; the existing `internal/fault/resource/*` implementations re-executed as `atropos-agent stress`.

**Behaviour changes to the in-process host:** `faultparams.ResourceCPU/Memory/IO` gain an optional `mechanism` (`""`|`burn`|`quota`); the in-process host executes `burn` natively and rejects `quota` at decode (it has no cgroup authority). `resource.Config` gains an optional `AvailableCPUs` / memory-bytes override used only by the agent's child (the child's own `/sys/fs/cgroup` view is its cgroup namespace, not the target's).

---

## File structure

```
faultparams/params.go                    Mechanism on ResourceCPU/ResourceMemory/ResourceIO (+Validate)
internal/fault/resource/config.go        AvailableCPUs override (cpu); memory: AvailableBytes override
internal/fault/resource/cpu/stress.go    honour Config.AvailableCPUs
internal/fault/resource/memory/*.go      honour Config.AvailableBytes
internal/instrument/decode.go            WithResourceHost hook; quota rejected without a host
internal/agent/api.go                    request/response shapes (shared by agent and client)
internal/agent/peercred.go / _linux / _other   caller pod UID from the socket peer
internal/agent/resolve.go                OCI-spec container resolver + cgroup finder (fs-only)
internal/agent/cgroup.go                 cgroup v2 reads/writes: cpu.max, memory.high, io.max, cgroup.procs
internal/agent/burn.go                   child re-exec, placement, go-signal, result
internal/agent/quota.go                  quota apply/restore + journal
internal/agent/server.go                 socket server: start/heartbeat/stop/list, watchdog
internal/agent/client.go                 AgentFault (fault.Fault) + ResourceFactory (decode hook)
internal/atroposd/daemon.go              agent socket → CapResource + WithResourceHost
cmd/atropos-agent/main.go                serve | stress
deploy/agent/{Dockerfile,daemonset.yaml,README.md}
```

---

### Task 1: `mechanism` param and detection overrides

- [ ] `faultparams`: `Mechanism string json:"mechanism,omitempty"` on `ResourceCPU`, `ResourceMemory`, `ResourceIO`; `Validate` accepts `""`, `"burn"`, `"quota"`; tests.
- [ ] `resource.Config.AvailableCPUs float64` (0 = detect) honoured by `cpu.Stress.Start`; `memory.Config.AvailableBytes int64` (0 = detect) honoured by `memory.Stress`. Tests: a `Stress` with the override plans workers from it, not from the host.
- [ ] `instrument.decode`: resource faults with `mechanism: quota` and no resource host → `UnsupportedFaultError{Category: "resource", FaultType: "<type>/quota"}`; `WithResourceHost(h ResourceHost)` where `type ResourceHost func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error)`; when set, every resource fault decodes through it (the sidecar never executes one itself).
- [ ] Commit — `feat(m4): resource mechanism param, detection overrides, resource decode hook`

---

### Task 2: agent core (resolver, cgroup, quota, burn) — pure-Go, fs-tested

- [ ] `resolve.go`: `type Target struct{ PodUID, Container, ContainerID, CgroupDir string }`; `Resolver{TaskRoots []string, CgroupRoot string}`; `Resolve(podUID, container)`: scan `<taskRoot>/k8s.io/*/config.json`, match annotations, then find the cgroup dir under `CgroupRoot` whose base name contains the container id. Tests with a temp tree (two containers of one pod + a pause container; a second pod).
- [ ] `cgroup.go`: `readCPUMax(dir) (quota, period)`, `writeFile`, `AvailableCPUs(dir)`, `AvailableMemory(dir)`, `Attach(dir, pid)` (`cgroup.procs`). Tests on temp files.
- [ ] `quota.go`: `Quota{Dir, Kind cpu|memory|io, Fraction}`; `Apply` writes the reduced value after journaling the original to `<journal dir>/<id>.json`; `Restore` writes it back and removes the journal; `RestoreAll(journalDir)` at boot. Tests: cpu.max `max 100000` × fraction 0.5 → `50000 100000`; `200000 100000` × 0.5 → `100000 100000`; memory.high from memory.max; restore round-trips; boot restore.
- [ ] `burn.go`: `StartBurn(ctx, t Target, req wire.FaultRequest, availCPUs float64, availBytes int64) (*Burn, error)`: `exec.Command(os.Args[0], "stress", ...)` with the fault JSON on stdin; child prints `ready\n`; parent attaches child pid to the cgroup, then writes `go\n`; child runs the fault (`decodeResourceFault` from a small shared decoder in `internal/agent/stressrun.go`) and prints the JSON result; `Stop` sends SIGTERM (child cancels its context). Test: helper-process pattern (`GO_ATROPOS_STRESS_CHILD=1`), temp `cgroup.procs`, a 150 ms cpu burn, assert the pid landed in the file before `go` and the result arrived.
- [ ] Commit — `feat(m4): agent core: container resolver, cgroup v2 quota with journal, burn placement`

---

### Task 3: agent socket server and sidecar client

- [ ] `api.go`: `StartRequest{PodUID, Container, Fault wire.FaultRequest}`, `StartResponse{ID}`, `Result{ActualDurationMs, Error, Detail}`; routes `POST /v1/faults` (streams: `{"id":..}\n` then, when done, the result line), `POST /v1/faults/{id}/heartbeat`, `DELETE /v1/faults/{id}`, `GET /v1/faults`.
- [ ] `peercred_linux.go`: `unix.GetsockoptUcred` on the accepted socket → pid → `/proc/<pid>/cgroup` → pod UID (`pod<uid>` segment; `_`→`-`). `peercred_other.go` stub. `Server.PeerPodUID` injectable (tests).
- [ ] `server.go`: `Server{Resolver, JournalDir, Grace, PeerPodUID}`; refuses `pod_uid != caller`; `burn` → StartBurn; `quota` → Quota.Apply with a heartbeat deadline (`Grace = max(3×30s, 30s)`… configurable) restored by a watchdog; stop → burn.Stop / quota.Restore; boot `RestoreAll`. Tests with a fake resolver, temp cgroupfs, injected peer: own-pod check, start/stop burn, quota apply/heartbeat-expiry restore, list.
- [ ] `client.go`: `AgentFault{Sock, PodUID, Container, Name, Req}` implements `fault.Fault` over the socket (`Validate`: socket reachable); `ResourceFactory(sock, podUID, container) instrument.ResourceHost`. Test against the in-process server on a temp socket.
- [ ] Commit — `feat(m4): atropos-agent socket server, own-pod peer check, sidecar AgentFault client`

---

### Task 4: daemon wiring, main, deploy

- [ ] `atroposd.Config`: `agent_socket` (default `/var/run/atropos/agent.sock`), `app_container` (default `server`); `Run`: if the socket is dialable at startup, add `CapResource` and `WithResourceHost(agent.ResourceFactory(...))`; pod UID from `MANTEION_INSTANCE_ID`… no: from the downward API env `ATROPOS_POD_UID` (manifest `metadata.uid`), else the caller's cgroup is authoritative on the agent side anyway (the sidecar sends what it has; the agent verifies).
- [ ] `cmd/atropos-agent/main.go`: `serve` (flags: `-sock`, `-cgroup-root /host/sys/fs/cgroup`, `-task-root ...` repeated, `-journal /var/lib/atropos-agent`) and `stress` (child).
- [ ] `deploy/agent/daemonset.yaml` (privileged, hostPID, hostPath mounts: cgroupfs rw, containerd task root ro, `/var/run/atropos` for the socket, journal dir), `Dockerfile.agent`, README with the M4 preconditions to verify on VM1.
- [ ] Commit — `feat(m4): atroposd delegates resource faults to atropos-agent; agent daemonset`

---

### Task 5: Docs

- [ ] README (resource faults in sidecar mode; `mechanism`), AGENTS (`mechanism` in the params catalogue), design §6 status, plan notes. Commit — `docs(m4): node agent`.

---

## Self-review

**Spec coverage (§5.14):** shape (DaemonSet, hostPID, cgroupfs, socket) — Task 4; mechanisms burn/quota with the `mechanism` param — Tasks 1–3; child placement before it starts, explicit CPU count — Task 2; quota restore on stop/restart/heartbeat loss — Tasks 2–3; local API and the own-pod peer check — Task 3; cgroup resolution — Task 2 (OCI specs + cgroupfs scan instead of a CRI client: same inputs, no gRPC dependency); telemetry via the sidecar's fault span and registry — Task 3 (`AgentFault` is a normal background fault); `disk` stays unadvertised. **Placeholder scan:** none. **Type consistency:** `ResourceHost`/`WithResourceHost` (Task 1) is what `ResourceFactory` (Task 3) returns and `Run` (Task 4) installs.
