# Daemon program, phase 3: plan index and execution order (2026-10-30)

Decisions: `2026-09-27-daemon-design-review-2.md` as amended by the user on 2026-10-30 (ADR-1: keep the
label matcher, add a central OPA harness; ADR-2 as recommended; ADR-3 and §5 in full). Contract:
`2026-10-30-desired-state-protocol.md`. Topology: host `10.0.0.250` (k3s: manteion, beds, Prometheus,
collector agent, sidecars, node agent) and the user's Mac (zeus, gateway collector, Tempo, Loki, Grafana).

| # | Plan | Repo / branch | Tasks | Depends on |
|---|---|---|---|---|
| A | `2026-10-30-sdk-quick-fixes-and-transient.md` | atropos-go `feat/atroposd` | 5 | — |
| B1 | `manteion-go/docs/plans/2026-10-30-desired-state-manteion-1-materializer.md` | manteion-go `feat/atropos-daemon` | 9 | protocol |
| C | `2026-10-30-desired-state-sdk.md` | atropos-go | 10 | A (apply path), protocol |
| B2 | `manteion-go/docs/plans/2026-10-30-desired-state-manteion-2-orchestrator.md` | manteion-go | 9 | B1, C (end-to-end) |
| D | `2026-10-30-telemetry-sdk.md` | atropos-go | 11 | A, C (decision emission sites) |
| E | `service-beds/docs/plans/2026-10-30-observability-stack.md` | service-beds `feat/atropos-daemon` | 12 | D (log exporter, endpoint form); cluster access for deploy tasks |
| F | `manteion-go/docs/plans/2026-10-30-opa-harness.md` | manteion-go | 9 | B1 (migration 12), B2 (migration 13) → F takes 14 |
| G | `2026-10-30-agent-netem.md` | atropos-go | 7 | C (decode rename), B1 (unknown capabilities ignored) |

Execution order: A → B1 → C → B2 → D → E → F → G. One plan at a time per repo; A/B1 and C/B2 pairs
may interleave across repos.

Cross-plan rulings (recorded here so they survive the per-plan ledgers):
1. `ReplaySetRef` carries `experiment_id`/`phase_id` (protocol amended 3631b2e).
2. Capability filtering ignores unknown capability strings (`netem` is advertised by the sidecar later).
3. migrations: B1 = 12, B2 = 13, F = 14 (F's text says 13; renumber at execution).
4. `/admin/rules` partial applies: the valid rules apply, the response is 422 with the per-rule errors
   (plan A's semantic; plan C's `applyReport` maps to it).
5. The push path (`atrocontrol.PushRules`, freeze, preload, intent tracker) is deleted in B2; the SDK's
   synchronous drain report inside the push handler (A, Task 1 review) therefore never blocks manteion.
6. Telemetry (D): the SDK never drops foreign baggage; it only adds its own keys. The standard Baggage
   propagator stays. Plan D's allowlisted propagator is reduced to "what the SDK itself injects".
7. Telemetry (D) keeps client_golang for metrics (exemplars + `service` label); no MeterProvider migration.
8. OTLP endpoint form after D: standard URL (`http://otel-collector:4317`); E's manifests follow D.
9. The committed OpenAPI spec is regenerated with `GOTOOLCHAIN=go1.25.5 make openapi` wherever a plan
   says `make openapi` (swag v2 cannot parse the Go 1.27 stdlib).
