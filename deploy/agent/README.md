# atropos-agent: node agent for resource faults

Resource faults for sidecar-instrumented pods run in the *target
container's* cgroup, which only a privileged per-node process can arrange
(design doc §5.14). The sidecar (`atroposd`) advertises the `resource`
capability when the agent's socket answers at startup and delegates every
resource rule to it for the pod's app container (`ATROPOSD_APP_CONTAINER`,
default `server`); the pod UID comes from the downward API
(`ATROPOS_POD_UID`). The agent verifies the caller's pod from the socket
peer regardless of what the request says.

Two mechanisms, selected per rule with `params.mechanism`:

- `burn` (default): the agent re-executes itself as a stress child, attaches
  the child to the target cgroup while it is parked, then lets it run the
  existing `internal/fault/resource` implementation with the target's
  capacity. Semantics, ramps and params are the in-process ones; the
  footprint is bounded by the target's own limit.
- `quota`: lowers `cpu.max` (cpu) or `memory.high` (memory) by
  `target_load` of the current capacity; journaled before it is written and
  restored on stop, when the sidecar disconnects, and at agent boot.
  Node-neutral (it removes capacity instead of consuming it). Not yet
  supported for `io`/`disk`.

## Deploy

```bash
docker build -f deploy/agent/Dockerfile -t atropos-agent:dev .
kubectl apply -f deploy/agent/daemonset.yaml
```

Sidecars must mount the same hostPath (`/var/run/atropos`) — the
`deploy/sidecar` kustomize patch mounts an `emptyDir` there for the capture
marker; switch that volume to the hostPath (or add a second mount for
`agent.sock`) on nodes running the agent. Restart the sidecar after the
agent is up: the capability is advertised at sidecar start only.

## Verify on the node first

```bash
stat -fc %T /sys/fs/cgroup                       # cgroup2fs required
ls /run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/*/config.json | head   # OCI specs present
sudo cat /run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/<id>/config.json | jq .annotations
sudo ls /sys/fs/cgroup/kubepods.slice/*/*/ | head # cri-containerd-<id>.scope dirs
```

## Containment

- Privileged, hostPID, host cgroupfs: the most powerful component in the
  mesh; declare it in every experiment spec.
- Own-pod check: `SO_PEERCRED` → `/proc/<pid>/cgroup` → pod UID; a request
  naming another pod is refused (403).
- Quota journal: `kill -9` of the agent leaves the journal; the next start
  restores every entry before serving.
- A fault lives as long as the sidecar's streaming request: sidecar gone,
  fault stopped / quota restored.
