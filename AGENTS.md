# atropos-go

Go SDK that each service embeds with `atropos.Serve` for fault injection, OpenTelemetry and the cache-box (record egress responses, replay them fail-closed when the service is frozen). The same core runs out of process as the `atroposd` sidecar (`cmd/atroposd`), with the `atropos-agent` node DaemonSet (`cmd/atropos-agent`) doing resource and netem faults. Every instance pulls its desired state from manteion.

## Workspace

<!-- Shared by the AGENTS.md of all five faults-lab repos; keep the copies in sync. Last verified 2026-11-16. -->
Five sibling git repos are checked out side by side in one workspace folder, written `faults-lab/` below (the folder itself is not a repo):

- `atropos-go`: Go SDK embedded in each service (faults, cache-box record/replay, OTel), plus the `atroposd` sidecar and the `atropos-agent` node agent.
- `manteion-go`: control plane in Go with Postgres. Holds rules and experiments and runs experiment phases.
- `zeus-go`: load generator. Runs k6 workflows and vegeta attacks when manteion asks.
- `service-beds`: the measured system, a Go port of Google's Online Boutique with atropos embedded.
- `manteion-ui`: React operator console for manteion.

zeus sends load to the bed. Each bed service embeds atropos, and every atropos instance long-polls manteion for its desired state; manteion never pushes rules. manteion calls zeus to start and stop load, and dials instance addresses only to read status and cache-box fidelity. The UI calls manteion's API (and, today, Prometheus directly).

Default checkouts lag by months. Before reading code, run `git worktree list` and `git branch -a --sort=-committerdate` and work in the newest tree. On 2026-11-16 those were: atropos-go `feat/atroposd` in `atropos-go/.claude/worktrees/elated-shannon-771fa0`; manteion-go and service-beds `feat/atropos-daemon` in `worktrees/manteion-atropos-daemon` and `worktrees/service-beds-atropos-daemon`; zeus-go `develop` in its main checkout; manteion-ui `mock-mode` in `manteion-ui/.claude/worktrees/mock-mode`. None of them was merged yet.

The lab is a single-node k3s on a home-LAN box at `10.0.0.250` (manteion, Postgres, the Go bed, Prometheus, an OTel collector agent). The owner's Mac runs zeus, the gateway collector, Tempo, Loki and Grafana. The UCSC VMs (vm1, vm2, `2262-cse115b-0*.be.ucsc.edu`) are retired, so docs, manifests and scripts that name them describe history. The box is unreachable off that LAN: run `nc -z -G 5 10.0.0.250 22` before any deploy step, and check on the host what is actually deployed.

The cross-repo knowledge base is `faults-lab/docs` (not under git). `research/` is still valid background. `ONBOARDING-TLDR.md`, `STATUS-2026-08-19.md`, `design/DESIGN-CALLS-cachebox-2026-07-30.md` and the generated `api/` pages predate the pull-based desired-state protocol and the new host; read them as history.

Remotes: `github` (github.com/microfaults, public) is the primary remote; `origin` (git.ucsc.edu) is legacy.

## Commands

These mirror `.gitlab-ci.yml` and the `Makefile`.

```bash
gofmt -l .                      # CI fails if this prints anything
go vet ./...
go build ./...                  # also: make build
go test -count=1 ./...          # also: make test
go test -race -count=1 ./...    # CI runs this on main/develop only
```

- OpenAPI: `GOTOOLCHAIN=go1.25.5 make openapi` regenerates `docs/swagger.{yaml,json}`; `make openapi-check` regenerates and fails on a diff. It needs `swag` v2.0.0-rc5 (`go install github.com/swaggo/swag/v2/cmd/swag@v2.0.0-rc5`) and python3 with PyYAML. The `GOTOOLCHAIN` pin matters because swag rc5 cannot parse the Go 1.27 standard library and this Mac has Go 1.27.
- The CI job `go-integration` clones manteion-go `develop` and builds it against this tree. That is the old push-era manteion, not `feat/atropos-daemon`, so a green job says nothing about the branch that consumes this protocol.

## Where the code misleads

- The module path `git.ucsc.edu/microfaults/atropos-go` is only an import path. The code lives on GitHub now; do not rename the module, every consumer imports it under that name.
- A change here reaches nothing until each consumer re-pins it. manteion builds locally against `worktrees/atropos-pin` (through its untracked `go.work`) and in Docker against `worktrees/atropos-go`, a symlink to `worktrees/atropos-beds-pin`; its CI clones atropos `develop`. The bed services build from their own vendored copy, which on 2026-11-16 matched `atropos-pin`, while the sidecar and agent images came from `atropos-beds-pin`. Three different atropos versions are in play at once.
- `types.go` aliases `internal/wire`, and manteion imports those types and `faultparams` directly. A wire change breaks manteion's build; a `faultparams` change changes which faults manteion's catalog accepts, depending on which pin manteion was built from.
- `network/packet_loss` and `latency` with `mechanism: netem` only run on an `atroposd` sidecar whose node agent advertises `netem`. manteion's capability filter drops the `netem` capability and filters by category only, so it sends these rules to every network-capable instance. The older vendored SDK in the beds fails to decode them; newer in-process hosts report them as unsupported.
- Reporting a rule as `unsupported` or under `errors` fails nothing. manteion's convergence barrier only checks the observed generation (and replay-set state on frozen services), so a phase whose fault no instance can run still converges and is recorded as a fault phase.
- `/admin/rules` and `/admin/fault` are short-lived overrides while an instance is connected. The next desired-state `200` replaces pushed rules (at most one 30 s long-poll hold later), and the fault watchdog drops an unconfirmed admin slot after about 90 s with default settings. A manual test against a connected instance therefore seems not to stick. manteion never calls these write endpoints; its client is read-only.
- A `409 phase_not_recording` answer to a cache-ingest batch is terminal: the SDK drops the batch without retrying. Records flushed after a recording phase stops running (for example when an operator pauses it) are lost.
- The SDK reads at most 1 MiB of a desired-state body (`io.LimitReader(resp.Body, 1<<20)` in `internal/instrument/manteion_client.go`). manteion has no matching cap, so an oversized rule set would fail to decode on every poll.
- `Config.Version` wins over `MANTEION_SERVICE_VERSION`. The bed services hard-code `Version: "0.1.0"`, so the env var their manifests set is ignored.
- The protocol contract `docs/plans/2026-10-30-desired-state-protocol.md` is wrong in two places: §4 says the kill switch writes a killed flag (manteion has none; it disables the service's rules and cancels its fault configs), and §5 says the bed vendors the SDK and upgrades together with it (not true today, see above).
- The README's "Current State" table says "shipped on `main`"; it describes this unmerged `feat/atroposd` branch.
- Metric names (`atropos_cachebox_hits_total`, `atropos_cachebox_misses_total`, `http_server_*`, ...) are what the Grafana dashboards in `service-beds/observability/` query. Renaming one breaks a dashboard silently.
- Everything in `docs/plans/` except the protocol contract is an executed plan, kept as history. `.agent/rules/sdk-rules.md` is the original project-intention note, not current rules.

## Conventions

- Commits use conventional-commit subjects with a scope, for example `feat(netem): ...` or `fix(agent): ...`. Make one commit per request and stage files by name, not with `git add -A`.
- The owner manages branches and merges. Do not push or open pull or merge requests unless asked; when asked to push, use `github`, not `origin`.
- `docs/` describes current reality only. Brainstorming specs and implementation plans go under `docs/superpowers/`, which is gitignored here and stays local; when work ships, update the knowledge-base doc instead of committing the plan.
- Handler changes need regenerated `docs/swagger.{yaml,json}` in the same commit; see `docs/openapi-conventions.md`.

## Read these

- `README.md`: embedding, the admin endpoints (Quick Start §4) and atroposd (§8).
- `docs/plans/2026-10-30-desired-state-protocol.md`: the SDK-manteion contract, with the two errors noted above.
- `deploy/sidecar/README.md` and `deploy/agent/README.md`: sidecar and node-agent deployment, including the k3s netem checks.
