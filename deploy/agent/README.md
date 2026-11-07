# atropos-agent: node agent for resource and netem faults

Resource faults for sidecar-instrumented pods run in the *target
container's* cgroup, which only a privileged per-node process can arrange
(design doc §5.14). The sidecar (`atroposd`) advertises the `resource`
capability when the agent's socket answers at startup and delegates every
resource rule to it for the pod's app container (`ATROPOSD_APP_CONTAINER`,
default `server`); the pod UID comes from the downward API
(`ATROPOS_POD_UID`). The agent verifies the caller's pod from the socket
peer regardless of what the request says. Netem network faults take the
same path (below).

Three mechanisms; the resource ones are selected per rule with
`params.mechanism`:

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
- `netem` (network faults: `packet_loss`, which is always netem, and
  `latency` with `params.mechanism: netem`): the agent enters the pod's
  network namespace through a process of the app container's cgroup
  (`nsenter --net=/proc/<pid>/ns/net`) and runs `tc` on the pod's egress
  interface (the first UP, non-loopback ether link of `ip -json link`,
  `eth0` when none is found). With a `network.target` the root is a 4-band
  `prio` qdisc (handle `1:`) whose priomap never selects band 4, the netem
  qdisc (handle `40:`) hangs off `1:4`, and one `u32` filter per IPv4
  address the target resolves to (plus a `dport` match when the target
  names a port) steers matching packets into it; with no target the netem
  qdisc is the root and shapes all of the pod's egress, the sidecar's own
  control-plane and OTel traffic included. The agent resolves the target
  name with its own resolver, so a short service name resolves only in the
  agent pod's namespace (use the FQDN otherwise), and only IPv4 addresses
  are used. `network.scope` multiplies `loss` and must lie in (0,1] (0
  means 1); latency needs scope 1. Egress only: `direction` must be empty
  or `upstream` (ingress would need an ifb device). Ramps are not
  supported: a non-zero `ramp_up_ms`/`ramp_down_ms` is refused. One netem
  fault per pod network namespace at a time: a second gets `409`; an
  invalid spec (a malformed target port or an IPv6 literal included) gets
  `400`; so does a `hostNetwork: true` pod, whose namespace is the node's
  (the fault would shape the whole node). Each fault is journaled under `-netem-journal` (default
  `/var/lib/atropos-agent/netem`, one `<id>.json`) before its first `tc`
  command, re-saved after each command that succeeds, and removed
  (`tc qdisc del dev <dev> root`) on stop, when the sidecar disconnects,
  at agent shutdown (SIGTERM: the agent releases every running fault before
  it exits) and at agent boot. The agent offers `netem` only when `tc` and `nsenter`
  are on its PATH (the image installs `iproute2` and `util-linux`).

`GET /v1/capabilities` answers `{"mechanisms": ["burn", "quota", "netem"]}`
(`netem` only when offered). The sidecar probes it at start: it advertises
`netem` and routes netem rules to the agent only when the list names it; an
agent that predates the endpoint answers 404, which the sidecar reads as
`burn` and `quota` only. Without a netem-capable agent (and always on the
in-process host) netem rules are skipped per rule and counted in
`atropos_rules_unsupported_total{category="network"}`.

## Deploy

```bash
docker build -f deploy/agent/Dockerfile -t atropos-agent:dev .
kubectl apply -f deploy/agent/daemonset.yaml
```

The Go build stages of `deploy/agent/Dockerfile` and
`deploy/sidecar/Dockerfile` run on the build host's platform
(`FROM --platform=$BUILDPLATFORM`) and cross-compile for the target with
`GOOS=$TARGETOS GOARCH=$TARGETARCH`, because emulating the Go compiler
under amd64 on an arm64 Mac segfaults it (`deploy/sidecar/Dockerfile.init`
has no Go stage). The agent's runtime stage installs `iproute2` and
`util-linux` and fails the build if `tc` or `nsenter` is missing.

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
modinfo sch_netem sch_prio cls_u32 | grep -c ^filename   # 3
```

## Containment

- Privileged, hostPID, host cgroupfs: the most powerful component in the
  mesh; declare it in every experiment spec.
- Own-pod check: `SO_PEERCRED` → `/proc/<pid>/cgroup` → pod UID; a request
  naming another pod is refused (403).
- Quota journal: `kill -9` of the agent leaves the journal; the next start
  restores every entry before serving.
- Netem journal: the same, for netem faults. A restore enters the pod's
  namespace by its identity (the `net:[inode]` journaled at apply), not by
  the app container's pid: when that pid is gone (the container restarted)
  it uses any live process holding the same namespace, the pod's pause
  process included; the entry is dropped without running `tc` only when no
  process holds it. Before deleting, a restore probes
  `tc qdisc show dev <dev> root` and runs `tc qdisc del dev <dev> root`
  only when the root is the agent's (`prio` or `netem`, handle `1:`); a
  root that is not (the default `noqueue`, a qdisc removed by hand, a
  device that is gone) means the fault is already removed, and the entry
  is dropped. The probe decides every entry, one whose journal re-save
  never happened included, so an agent crash between the root
  `qdisc add` and that re-save is restored at the next boot (there is no
  manual-removal residual). When removing a fault fails (the rollback of a
  failed apply, a stop, or the boot restore), the entry is kept and the
  namespace stays reserved (a new netem fault there gets `409`), so a later
  stop (`DELETE /v1/faults/<id>` consults the journal) or the next agent
  boot retries the removal. A restore is single-flight per fault id (a
  concurrent `DELETE` of the same id answers `200` without running `tc`)
  and never touches a namespace a newer fault holds: such a stale entry is
  dropped without `tc`.
- A fault lives as long as the sidecar's streaming request: sidecar gone,
  fault stopped / quota restored / netem removed.

## Verify netem on a k3s node

Unit tests stub `nsenter`/`tc`; this is the real check, on VM1 after the
agent DaemonSet and a sidecar-instrumented service are up. This recipe has
not been executed on a cluster: it was checked by reading it against the
code (the service-beds runbook execution is outstanding).

```bash
POD=$(sudo k3s kubectl get pod -l app=recommendationservice -o jsonpath='{.items[0].metadata.name}')
BEDS=$(sudo k3s kubectl get pod $POD -o jsonpath='{.metadata.namespace}')
PODIP=$(sudo k3s kubectl get pod $POD -o jsonpath='{.status.podIP}')
CID=$(sudo k3s crictl ps --name '^server$' --pod $(sudo k3s crictl pods --name $POD -q) -q)
PID=$(sudo k3s crictl inspect $CID | jq .info.pid)
CATALOG=$(sudo k3s kubectl get svc productcatalogservice -n $BEDS -o jsonpath='{.spec.clusterIP}')
CATALOG_EP=$(sudo k3s kubectl get endpoints productcatalogservice -n $BEDS -o jsonpath='{.subsets[0].addresses[0].ip}')
FQDN=productcatalogservice.$BEDS.svc.cluster.local   # the agent resolves names in its own pod's DNS namespace
NS="sudo nsenter --net=/proc/$PID/ns/net"

$NS tc qdisc show dev eth0                          # before: "qdisc noqueue 0: root"

# 1. Targeted loss (30% toward productcatalogservice:3550) through the sidecar's admin slot.
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"packet_loss","duration_ms":120000,
  "network":{"target":"'$FQDN':3550","direction":"upstream"},
  "params":{"loss":30,"correlation":25}}'
$NS tc -s qdisc show dev eth0                       # prio 1: root, netem 40: parent 1:4 "loss 30% 25%"; its "dropped" counter rises under load
$NS tc filter show dev eth0                         # u32, flowid 1:4: match <CATALOG in hex>/ffffffff at 16 and 00000dde/0000ffff at 20 (dport 3550)
sudo ls /var/lib/atropos-agent/netem                # one <id>.json
curl -s -XDELETE http://$PODIP:16020/admin/fault
$NS tc qdisc show dev eth0                          # back to noqueue; journal dir empty

# 2. Loss visible to ping. A ClusterIP answers no ICMP under kube-proxy's
#    iptables mode (the k3s default: it is DNAT'd only for its service ports),
#    so target an endpoint pod IP, without a port (ICMP has no dport).
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"packet_loss","duration_ms":120000,
  "network":{"target":"'$CATALOG_EP'","direction":"upstream"},"params":{"loss":30}}'
$NS ping -c 50 -i 0.2 -W 1 $CATALOG_EP | tail -2   # ~30% packet loss
curl -s -XDELETE http://$PODIP:16020/admin/fault

# 3. Kernel latency, whole pod: measured over TCP to the service port (the
#    ClusterIP answers TCP on 3550, not ICMP), or ping the endpoint pod IP.
for i in 1 2 3 4 5; do $NS curl -o /dev/null -s -w '%{time_connect}\n' http://$CATALOG:3550/; done   # before: well under 0.01 s
curl -s -XPOST http://$PODIP:16020/admin/fault -H 'content-type: application/json' -d '{
  "category":"network","fault_type":"latency","duration_ms":60000,
  "network":{"direction":"upstream"},"params":{"delay":"200ms","jitter":"50ms","mechanism":"netem"}}'
for i in 1 2 3 4 5; do $NS curl -o /dev/null -s -w '%{time_connect}\n' http://$CATALOG:3550/; done   # ≈ 0.2 s each (± jitter): the SYN is delayed
$NS ping -c 5 $CATALOG_EP | tail -1                 # rtt avg ≈ 200 ms (± jitter)
curl -s -XDELETE http://$PODIP:16020/admin/fault

# 4. Crash restore: arm 1 again, then SIGKILL the agent (crictl stop with no grace period).
sudo k3s crictl stop --timeout 0 $(sudo k3s crictl ps --name '^agent$' -q)
sudo k3s kubectl logs -l app=atropos-agent | grep "restored journaled netem"       # one line, from the restarted container's boot
$NS tc qdisc show dev eth0                          # noqueue
curl -s -XDELETE http://$PODIP:16020/admin/fault    # the slot is still armed: clear it before a later delivery re-arms it
```

Step 4 kills the agent with SIGKILL, so nothing of the agent runs at exit
and the qdisc and its journal entry are left for the restarted container's
boot restore, which removes the qdisc and logs one `restored journaled
netem` line per entry. The `ping`s and `curl`s run from the node's own
binaries inside the pod's namespace, so they do not depend on the app image
shipping them; `tc` shapes the namespace's egress whichever process sends.
Targets use the FQDN (or an IP): the agent resolves a target name with the
resolver of its own pod, so a short service name resolves only when the
agent DaemonSet runs in the beds' namespace.

Transparent capture: the app's connections are REDIRECTed to the sidecar on
loopback and the sidecar dials the ClusterIP from the same namespace, so the
targeted filter shapes the sidecar→dependency leg; the app observes slow or
failing responses from the sidecar. The loopback hop is never shaped.
