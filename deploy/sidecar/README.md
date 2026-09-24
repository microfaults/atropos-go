# atroposd sidecar: deploy assets

The out-of-process host of the atropos instrument (design:
`docs/plans/2026-09-08-atropos-daemon-design.md`). One sidecar per pod;
registers with manteion as the pod's SDK instance; the app's embedded
`Serve` (if any) goes passive with `ATROPOS_HOST=sidecar`.

| File | Purpose |
|---|---|
| `Dockerfile` | `atroposd` image (build from the repo root), runs as uid 1337 |
| `Dockerfile.init` + `init.sh` | init container: iptables REDIRECT rules + capture marker (transparent mode) |
| `kustomize/recommendationservice-sidecar.yaml` | strategic-merge patch adding both containers to one Go-port service |

## Build

```bash
docker build -f deploy/sidecar/Dockerfile      -t atroposd:dev .
docker build -f deploy/sidecar/Dockerfile.init -t atropos-init:dev deploy/sidecar
```

## Verify the node first (M2 preconditions, design §5.2)

On VM1 (k3s), before the first rollout:

```bash
uname -r                              # kernel; conntrack/nat REDIRECT
stat -fc %T /sys/fs/cgroup            # cgroup2fs expected (node agent, M4)
sudo k3s --version                    # >= 1.29 for native sidecar containers (initContainers[].restartPolicy: Always)
sudo k3s kubectl get ns default -o jsonpath='{.metadata.labels}'   # PodSecurity must allow NET_ADMIN
sudo k3s kubectl run -it --rm ipt --image=alpine:3.22 --restart=Never -- sh -c 'apk add -q iptables && iptables -w 5 -t nat -S'
```

The init image uses the `iptables` package's nft backend; if the node's
kube-proxy uses legacy iptables, switch with `update-alternatives` inside
the image (or install `iptables-legacy`) so both write the same tables.

## Roll out one service

1. Add the patch to a kustomize overlay of the service-beds manifests and
   apply it. The app container gains `ATROPOS_HOST=sidecar`; the sidecar
   registers under the pod name with `host: sidecar` and
   `capabilities: [inline, network, cachebox]`.
2. Check `http://<pod-ip>:16020/atropos/health` → `"host":"sidecar",
   "capture":"transparent"` and `"status":"connected"`; manteion's instance
   list shows the same pod once (the passive embed does not register).
3. Point Prometheus at `:16020/metrics` for the atropos series. Watch
   `atropos_proxy_overhead_seconds{point}` (G-proxy, design §5.13) and
   `atropos_rules_unsupported_total` (resource rules are skipped by the
   sidecar until the node agent lands).
4. Non-HTTP dependencies (redis, kafka, postgres) must be listed in
   `ATROPOSD_RAW_TCP_PORTS` so their connections are relayed at L4 instead
   of parsed as HTTP; the OTel collector ports stay excluded from the
   redirect (`ATROPOSD_EXCLUDE_OUTBOUND_PORTS`).

Explicit mode (no init container, no NET_ADMIN) remains available for
docker-compose and bring-up: set `ATROPOSD_CAPTURE=explicit`,
`ATROPOSD_EGRESS=":19090=productcatalogservice:3550,..."`, and point the
app's `*_SERVICE_ADDR` at `127.0.0.1:19090`.

## Roll back

Remove the patch from the overlay and re-apply: the pod returns to one
container with the embedded host active (`ATROPOS_HOST` unset). No
manteion state needs cleaning; the sidecar deregisters on SIGTERM.

## Known gaps

- IPv4 only (`SO_ORIGINAL_DST`); IPv6 original destinations are a follow-up.
- The startup capture check is the marker file; a probe from the app's uid
  through the redirect is a follow-up.
- Resource faults: not executed by the sidecar (node agent, M4).
