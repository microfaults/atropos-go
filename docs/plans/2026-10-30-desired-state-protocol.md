# Desired-state protocol v1 (ADR-2 contract between manteion and the SDK/sidecar)

> Status: accepted 2026-10-30 (user decision on ADR-2 of `2026-09-27-daemon-design-review-2.md`).
> This file is the contract. The manteion plan and the SDK plan implement it independently; wire types live in
> atropos-go `internal/wire` (root aliases) and manteion imports them.

## 1. Model

- manteion materializes `DesiredState(service, instance)` as a pure function of the database: enabled rules
  gated by running phases (as `ForService` does today), active fault configs, the synthesized cache-box rule
  for the service's current cache-box role, the freeze delay config of a frozen phase, the replay-set
  artifact of a frozen phase, all **filtered by the instance's capabilities** (`inline|network|resource|
  cachebox`; a cache-box rule needs `cachebox`; absent capabilities = all). Compositions are shipped as today.
- Each service has a **generation** (`service_generations(service TEXT PRIMARY KEY, generation BIGINT NOT NULL
  DEFAULT 1, updated_at TIMESTAMPTZ)`), bumped in the same transaction as any input change: rule CRUD, fault
  config fire/cancel/reap, phase enter/exit/drain/teardown for every service in the phase, freeze/thaw,
  artifact assignment, an instance registering with different capabilities, kill switch. The in-memory
  `IntentTracker` is deleted; a manteion restart changes nothing.
- The **ETag** of a materialized document is `"<generation>-<sha256 hex[:16] of the canonical JSON>"`.
  Generation alone is not enough: an instance can see the generation move while its own document is
  unchanged (another instance's capabilities changed); the hash tells it not to re-apply.

## 2. Endpoints

### `GET /api/v1/sdk/rules?service=S&instance_id=I&version=G&wait=30`
- Unchanged path. `version` is now the **generation the instance holds** (its observed generation); manteion
  records it on `sdk_instances.observed_generation` together with `last_poll_at` before anything else (this
  is how convergence is observed; no extra round trip in the common case).
- Request header `If-None-Match: "<etag>"` (the ETag of the last 200 the instance applied, or of the last
  304 it confirmed).
- `wait` (seconds, default 0, max 60): long-poll. manteion holds the request until the service's generation
  changes or `wait` elapses, re-materializes, and compares ETags.
- `200` with body `RuleSync` and headers `ETag`, `X-Atropos-Generation: <G>`.
- `304` with headers `ETag`, `X-Atropos-Generation: <G>` when the ETag matches after the hold. On 304 the
  instance sets its observed generation to `G` without re-applying (its document is provably unchanged).
- Errors as today (`application/problem+json` on the branch's error envelope).

`RuleSync` (wire, atropos-go `internal/wire/sync.go`):
```json
{
  "version": 42,                       // the generation (kept under the old name)
  "etag": "42-9f1c0e3a7b2d4f60",
  "rules": [ /* CompiledRule, never null */ ],
  "active_faults": [ /* FaultRequest, never null */ ],
  "freeze_cfg": { "mu": 0.0, "sigma": 0.0, "seed": 7, "context": { "experiment_id": "...", "phase_id": "..." } },
  "replay_set": { "id": "sha256:...", "experiment_id": "...", "phase_id": "...",
                  "key_strategy": "canonical_v2", "count": 1234, "sha256": "...",
                  "size_bytes": 5242880, "url": "/api/v1/cache/artifacts/sha256:..." }
}
```
`replay_set.experiment_id` and `phase_id` name the pair the set is installed under (the cache-box serves a
replay set only to the rule whose context matches that pair; a set without a pair is unusable).
`freeze_cfg` and `replay_set` are `omitempty`; absent means "none desired": the instance clears its delay
source / replay set if it holds one from an earlier state.

### `POST /api/v1/sdk/status`
Body:
```json
{ "instance_id": "I", "service": "S", "applied_generation": 42, "etag": "42-...",
  "unsupported": [ { "rule": "cpu-quota", "category": "resource" } ],
  "errors": [ "rule x: decode: ..." ],
  "replay_set": { "id": "sha256:...", "state": "committed" } }
```
`204`. Sent by the instance after every apply (200 path) and whenever `replay_set.state` changes
(`fetching|committed|failed`). The report is a **full snapshot** of what the instance holds: `unsupported`
and `errors` replace the previous report (empty list clears), and `replay_set` names the set the instance
currently holds, so an absent `replay_set` means "none" and clears the stored id and state.
manteion stores them on `sdk_instances` (`observed_unsupported JSONB`, `observed_errors JSONB`,
`replay_set_id TEXT`, `replay_set_state TEXT`, `observed_at`).

### `GET /api/v1/cache/artifacts/{id}`
Immutable replay-set artifact (NDJSON, one `CacheBoxWireEntry` per line, the same lines preload chunks
carried). Supports `Range: bytes=a-b` (206) and `HEAD` (`Content-Length`, `ETag: "<id>"`). `id` is
`sha256:<hex>` of the file bytes; `sha256` in `replay_set` is the W5 digest over entries (unchanged
algorithm), `count` the entry count. The instance fetches in ≤4 MiB ranges, stages entries through the
existing preload staging path, and commits when count and W5 digest match; any failure → `failed` in the
status report with the reason in `errors`.

### `GET /api/v1/sdk/events?service=S` (SSE)
Unchanged shape; the materializer emits `rules_changed {"generation":G}` on every generation change
(including orchestrator transitions, which never emitted before). Still a nudge; long-poll is primary.

### `POST /api/v1/cache/ingest`
Body gains `epoch` (int64, the instance's start time in unix nanoseconds). Dedupe key becomes
`(experiment, phase, instance, epoch, batch_seq)`. A body without `epoch` is treated as `epoch = 0`
(old clients) and keeps the old behaviour.

### Register
`POST /api/v1/sdk/register` persists `host` (`in-process|sidecar`, column `host_kind TEXT`) and
`capabilities` (`JSONB`). The response body is the materialized `RuleSync` (same shape as a 200 poll)
plus `status`. Registering with capabilities different from the stored ones bumps the service generation.

## 3. Instance behaviour (SDK and sidecar, shared core)
- Loop: `wait=30` long-poll with `If-None-Match`; on 200 apply through the single `apply()` path (the path
  `/admin/rules` POST must also use: drain observation and generation bookkeeping happen once), then POST
  status, then poll again immediately; on 304 adopt `X-Atropos-Generation`; on error back off as today.
- Unsupported rules and compositions are skipped **per rule**, counted in
  `atropos_rules_unsupported_total{category}` (compositions under `category="composition"`), and reported.
- `freeze_cfg.context` is honoured: the delay source is installed scoped to `(experiment_id, phase_id)` and
  exposed in `/admin/cachebox` stats.
- Replay set: when `replay_set.id` differs from the committed one, fetch → stage → commit; report state
  transitions. A restarted instance converges on its own.

## 4. Orchestrator behaviour (manteion)
- Phase entry: write the inputs (rules attached, artifact assigned, freeze config), bump the generation of
  every service in the phase once, then **wait for convergence**: every live instance of those services
  has `observed_generation >= target` and, for frozen services, `replay_set_state = committed` for the
  assigned artifact, within `MANTEION_CONVERGE_TIMEOUT` (default 30 s). Timeout fails the phase with the
  list of lagging instances. Replaces `pushPhaseRules`, `freezeServices` and `preloadCacheEntries`.
- Phase exit / drain / teardown: same bump-and-wait with a non-fatal timeout (logged).
- Kill switch: writes the killed flag (desired state = no rules, no faults), bumps, and additionally keeps
  the existing immediate push as a best-effort nudge.
- `atrocontrol.PushRules`, `FreezeService`, `ClearService`, `PreloadService` and the intent tracker are
  removed once the orchestrator no longer calls them; `InjectFault*`/`ClearFault*` (no callers) go with
  them. Fidelity pull and drain stay.

## 5. Compatibility
- Only the branch SDK talks to the branch manteion (the Go bed vendors the SDK; upgrade together). The
  `version` field keeps its name so the old SDK still parses the body; it will simply never long-poll.
