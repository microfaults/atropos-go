# Engineering Ambiguities — Cache-Box Stage 1

Living list of decisions I made during Stage 1 implementation that I wasn't 100% sure about. Each entry has the location, the choice I made, the reasoning at the time, and what might warrant revisiting.

These are *not* bugs. They're judgment calls where the better answer depends on information I don't have yet (workload data, manteion integration details, paper framing).

---

## A1 — Hand-rolled `sortStrings` instead of `sort.Strings`

**Location:** `internal/cachebox/key.go:135` (`sortStrings`)

**Decision:** Used a five-line insertion sort instead of importing `sort` from the stdlib.

**Reasoning at the time:**
- The `cachebox` package is stdlib-dep-minimal by design (no external deps, and only the smallest subset of the stdlib).
- Insertion sort is actually faster than `sort.Strings` for slices <~20 elements (measured in general Go benchmarks, not this code).
- Query strings with >20 parameters are rare in the workloads we care about.

**Why it's worth revisiting:**
- The argument against importing `sort` is aesthetic, not functional. `sort` is stdlib; the cost is zero.
- If we ever sort longer lists (header normalization, label maps), we'll want `sort.Strings` anyway.
- The five-line implementation is a maintenance liability relative to "just import sort."

**Revisit when:** any other part of `cachebox` needs sorting.

---

## A2 — Query params sorted alphabetically for cache key normalization

**Location:** `internal/cachebox/key.go:123` (`normalizeQuery`)

**Decision:** Treat `a=1&b=2` and `b=2&a=1` as the same cache key.

**Reasoning at the time:**
- For the vast majority of APIs, query param order is semantically meaningless.
- Normalizing increases cache hit rate without hurting correctness.
- Clients (including Go's `http` package) don't guarantee order stability when encoding a `url.Values` map, so two "identical" calls from the same caller might produce different orderings. Without normalization the cache would miss on those.

**Why it's worth revisiting:**
- **Some APIs *are* order-sensitive.** OAuth1 signature base strings. OData `$top`/`$skip` pairs. Legacy SOAP-over-HTTP-with-querystring shims. AWS SigV4 canonical request.
- Atropos is a research SDK being pointed at Online Boutique and similar modern HTTP APIs — the edge cases above are unlikely to show up in experiments. But if we ever run it against a legacy stack, this will silently cause false cache hits.

**Revisit when:** we care about query-order-sensitive APIs. Likely never in the research context, but worth a `KeyStrategyExactPreserveOrder` if this becomes a production concern.

---

## A3 — `handleCacheBox` unconditionally defers `span.End()`

**Location:** `internal/interceptor/cachebox.go:40`

**Decision:** `defer span.End()` at the top of `handleCacheBox`, regardless of which branch is taken, regardless of context cancellation, regardless of error.

**Reasoning at the time:**
- Fault injection uses `EndWithError` on failure paths to mark spans as errored, but cache-box "failures" are really just cache misses (which fall through to passthrough) or oversize responses (which still succeed from the caller's POV). There's no "error" outcome that should mark the span red.
- Deferred `span.End()` is the idiomatic Go pattern and makes it easy to reason about span lifetime.

**Why it's worth revisiting:**
- There's no cache-box equivalent of `span.RecordResult(fault.Result)`. The span just gets the events (`cachebox.record` / `cachebox.replay` / `cachebox.miss` / `cachebox.oversize`) but no structured "outcome" attribute.
- When we later want to compute hit rates from traces, we'll want a `cachebox.outcome` enum attribute set at the end.
- Context cancellation during `replay_with_delay` sleep returns `ctx.Err()` but doesn't mark the span as errored — arguably it should.

**Revisit when:** we start doing trace-based analysis of cache-box effectiveness (Stage 3, post-manteion).

---

## A4 — Response body slice aliased between caller and cache entry

**Location:** `internal/interceptor/cachebox.go:149-164`

**Decision:** `body := buf.Bytes()` is handed to both the caller's `resp.Body` (wrapped in a `NopCloser` reader) and to the recorder as `ResponseBody: body`. The recorder puts it into the cache entry. One slice, two owners.

**Reasoning at the time:**
- The caller is expected to *read* `resp.Body`, not mutate the underlying bytes. Go's `io.Reader` contract doesn't say anything about mutation, and the stdlib HTTP transport doesn't pass around mutable byte slices either.
- Cloning the body would double the memory footprint on every cached request, which is wasteful when the caller reads and discards.
- The `Entry` doc comment already says "immutable after Put" for both Header and Body.

**Why it's worth revisiting:**
- "Documented immutable" is weaker than "enforced immutable." A buggy middleware downstream of the interceptor could mutate the body and corrupt the cache.
- The vulnerability window is: record path finishes → caller reads/modifies → replay path serves stale bytes. Narrow but real.
- Trade-off: memory vs. correctness. Under heavy cache-box traffic the double-copy cost is ~1 MiB × cache size → potentially tens of MiB. Probably still fine on modern hardware.

**Revisit when:** we see evidence of cache corruption in experiments, OR the double-copy cost becomes measurable in profiles.

---

## A5 — Anonymous `struct { io.Reader; io.Closer }` for oversize passthrough

**Location:** `internal/interceptor/cachebox.go:136-142`

**Decision:** When a response exceeds `MaxBodyBytes`, we rebuild `resp.Body` with an anonymous struct that embeds a `MultiReader` as `Reader` and the original body as `Closer`. This is necessary because we already consumed part of the stream with `io.CopyN`, and we need to hand the caller a `ReadCloser` that both (a) replays the peeked bytes, (b) streams the rest, and (c) closes the original underlying body on Close.

**Reasoning at the time:**
- Anonymous struct embedding is the canonical Go idiom for "give me a ReadCloser from a separate Reader and Closer."
- Extracting a named type (`multiReadCloser`?) for one use site felt like over-engineering.

**Why it's worth revisiting:**
- Anonymous-embedded interface structs are notoriously ugly. Reviewers trip over them.
- If we ever need the same pattern elsewhere (e.g. ingress cache-box oversize handling), a named type becomes the right call.
- `httputil.DrainBody` does something similar-ish and could be a reference pattern.

**Revisit when:** a second use site appears, OR someone on code review flags it.

---

## A6 — `DistributionDelaySource` PCG seed derivation

**Location:** `internal/cachebox/delay.go:61`

**Decision:** `rand.NewPCG(seed, seed ^ 0x9E3779B97F4A7C15)`. The golden ratio XOR trick derives a second seed from the first so callers only need to pass one `uint64`.

**Reasoning at the time:**
- `rand.NewPCG` wants two seeds. Forcing callers to supply two was UX noise.
- The golden-ratio XOR is a common trick in hash/RNG seeding (used in boost::hash_combine, Go's `maphash`, etc.) and gives reasonable decorrelation for the two halves of the PCG state.

**Why it's worth revisiting:**
- **If callers pass the same seed across SDKs**, all SDKs sample identical delay sequences. For experiments this is bad: every pod in a deployment injects the exact same synthetic delay on the same request, which is an observable artifact.
- Stage 3 (manteion push) will need to define a per-SDK seed derivation strategy — `hash(hostname) ^ pid` or similar. The current code puts the burden on the caller.
- Zero is a valid input but produces a deterministic, low-entropy starting state (`NewPCG(0, 0x9E3779B97F4A7C15)` is fine mathematically but signals "forgot to seed" to a reader).

**Revisit when:** Stage 3 manteion integration. The right place is probably a helper like `NewDefaultDistributionDelaySource()` that derives a seed from process identity.

---

## A7 — `Interceptor.Check()` silently no-ops on cache-box decisions

**Location:** `internal/interceptor/interceptor.go:137-148`

**Decision:** `Check()` (the fault-only convenience) returns `(CheckResult{}, nil)` when the evaluator returns a cache-box decision. The caller is not told the decision existed. It's documented as "middleware that handles cache-box uses `Evaluate` directly."

**Reasoning at the time:**
- Splitting the API cleanly is more important than preserving the old `Check()` contract exactly.
- Existing fault-only callers (`IngressMiddleware`, gRPC interceptors) should not need to know cache-box exists.
- Returning an error would force every existing caller to add an `errors.Is(err, ErrCacheBoxIgnored)` ladder.

**Why it's worth revisiting:**
- Silent-drop is a footgun. If someone writes a rule that attaches a cache-box action to an ingress request, nothing happens and there's no log, no metric, no span event explaining why.
- At minimum, there should be a one-shot warning log the first time this happens per process: "cache-box decision dropped at ingress/unsupported transport: %v".
- Better: the `StaticEvaluator` should refuse to return cache-box decisions for `Request.Point == Ingress` in Stage 1. Push the error up to rule validation time.

**Revisit when:** we have a story for per-transport rule validation (likely when adding ingress cache-box).

---

## A8 — Warmth-score metric is a distinct-key count, not a ratio

**Location:** `internal/cachebox/recorder.go` (`RecorderStats.Recorded`) + `internal/cachebox/memstore.go` (`StoreStats.Entries`)

**Decision:** "How warm is the cache?" is answered with `RecorderStats.Recorded` (lifetime count) and `StoreStats.Entries` (current distinct count). No ratio of `observed / expected` because we don't know `expected`.

**Reasoning at the time:**
- The expected key set can only be supplied externally (by manteion, from a previous baseline run, or by the rule author).
- Exposing raw counters is honest — downstream analysis can compute whatever ratio it wants once it knows the denominator.

**Why it's worth revisiting:**
- Operators looking at a single SDK's stats can't tell if "1000 recorded keys" is healthy or not.
- A proper warmth score is a deferred-to-Stage-3 item but worth prototyping in Stage 2 as "expected key set = set from last manteion bundle."

**Revisit when:** Stage 2 (manteion scaffolding) — this is where the expected-set ground truth becomes available.

---

## A9 — `atropos.workflow` label extraction with no workflow taxonomy

**Location:** `internal/interceptor/middleware.go:extractHTTPLabels`

**Decision:** `extractHTTPLabels` reads the `atropos.workflow` member from W3C Baggage and emits it as a label, even though Stage 1 has no concrete workflow taxonomy wired up.

**Reasoning at the time:**
- Adding the extraction now means rule authors can experiment with per-workflow scoping (`{atropos.workflow: browse}` in a `StaticRule.Labels`).
- Doing it later means we have to ship a second middleware change in a follow-up PR.

**Why it's worth revisiting:**
- There's no tooling to set the baggage member — zeus-go sets `meta-trace-id` but not `atropos.workflow`. Rule authors who try to use this label will see no matches and get confused.
- Either (a) teach zeus-go to set `atropos.workflow` from flow JSON, or (b) document that this label is a placeholder.

**Revisit when:** we start running per-workflow isolation experiments (likely Stage 3, once the analysis path is built).

---

## A10 — Fault vs. cache-box priority is unresolved

**Location:** N/A (design, not code)

**Decision:** In Stage 1, the `StaticEvaluator` matches the first rule and returns its decision. If a rule has `Fault != nil`, you get a fault; if it has `CacheBox != CacheBoxNone`, you get cache-box. They're mutually exclusive by rule-author convention, not by type.

**Reasoning at the time:**
- The `StaticEvaluator` is for unit tests and bootstrapping; it's not the production rule engine.
- Mutual exclusion at rule-author level is simpler than introducing priority ordering on a per-decision basis.

**Why it's worth revisiting:**
- When an OPA-based evaluator enters the picture, policies can produce structured output with both `fault_config` and `cachebox_action`. The SDK needs a tiebreaker rule, or it needs to reject the policy at load time.
- The ergonomic answer is probably "cache-box wins over fault injection" (frozen service cannot also be fault-injected — by definition its handler is not running), but we should state this explicitly.

**Revisit when:** OPA evaluator lands (post-Stage 2).

---

## A11 — Default `MaxBodyBytes = 1 MiB`

**Location:** `internal/cachebox/cachebox.go` (`DefaultMaxBodyBytes`)

**Decision:** 1 MiB cap on cached response body size.

**Reasoning at the time:**
- Most REST responses in our target workloads (Online Boutique, similar) are well under 100 KiB.
- 1 MiB gives generous headroom without risking unbounded memory growth under a recording workload.
- Oversized responses fall through without caching, so the cap is a soft limit on what gets stored, not a hard limit on what the SDK can pass through.

**Why it's worth revisiting:**
- This is pulled out of thin air. No data.
- Services that return image blobs, ML embeddings, or paginated result sets could easily exceed 1 MiB and get silently un-cached, which looks like a cache-box bug from the experimenter's perspective.
- We should surface `cachebox.oversize` counts prominently so operators notice.

**Revisit when:** we have actual response size distributions from real workloads.

---

## A12 — Request body capture for `exact_with_body` changes semantics for streaming POST

**Location:** `internal/cachebox/cachebox.go` (`BufferRequestBody`)

**Decision:** When `KeyStrategy == KeyStrategyExactWithBody`, the SDK fully reads the request body up to `MaxBodyBytes`, hashes it, then replaces `r.Body` with a `NopCloser` over the captured bytes.

**Reasoning at the time:**
- There's no way to hash a stream without consuming it.
- Most POST bodies in the target workloads are small JSON payloads (<10 KiB).
- This strategy is opt-in only — callers who enable it are accepting the trade-off.

**Why it's worth revisiting:**
- A caller that uploads a multi-gigabyte file via POST and has `exact_with_body` enabled will have their upload silently buffered into memory up to the cap, then truncated, then passed through with a rebuilt body that concatenates the captured prefix with the rest of the stream.
- The semantics are correct (full body is sent) but the memory cost is proportional to `MaxBodyBytes` per in-flight request.
- Documentation on this edge case is in the code comments but not user-visible anywhere.

**Revisit when:** someone tries to use cache-box on a POST endpoint with large bodies. The fix is probably a `KeyStrategyExactWithBodyPrefix(n)` that only hashes the first N bytes.

---

## A13 — No PushFunc wiring to manteion

**Location:** `internal/cachebox/recorder.go` (`PushFunc`) — defined but always nil in Stage 1.

**Decision:** The `PushFunc` hook exists on `RecorderConfig` but `atropos.WithCacheBoxCoordinator` doesn't take one. Stage 1 SDKs keep everything in the local `MemStore`.

**Reasoning at the time:**
- Stage 1 is self-contained on purpose. Pushing to manteion requires manteion to exist first, and manteion-go is empty.
- The hook is in place so Stage 3 integration is a single wiring change, not a refactor.

**Why it's worth revisiting:**
- The contract for when `PushFunc` is called (every record? batched? only on LRU eviction?) isn't specified. Stage 3 will need to decide.
- The function signature assumes synchronous delivery. If manteion is slow/down, the drain goroutine stalls and the channel backs up → dropped records. We'll need a secondary queue or batch sender.

**Revisit when:** Stage 3 (manteion integration).

---

## A14 — Ingress cache-box is not implemented

**Location:** N/A (scope decision)

**Decision:** Stage 1 ships egress cache-box only. `IngressMiddleware` still uses the fault-only `Check()` convenience.

**Reasoning at the time:**
- Ingress cache-box requires a `ResponseRecorder` wrapping `http.ResponseWriter` to buffer handler output. More invasive than egress's `http.RoundTripper` wrap.
- Egress is sufficient for the "freeze dependencies of a workflow" experiment pattern.
- Cut scope to get Stage 1 shippable.

**Why it's worth revisiting:**
- Ingress cache-box is the right vehicle for **modeling service-internal cache state** (warm/cold tiers, Redis hit ratio, JIT warmth). Egress freezes the *dependency graph*; ingress freezes the *service's own compute*. They're orthogonal and together give a 2D experiment matrix.
- Metastable failure studies specifically want ingress `replay_with_delay` with a swept delay parameter to simulate progressively colder internal caches without needing to prime real ones.
- This is a Phase 1.5 deliverable — after egress is proven in experiments, before OPA integration.

**Revisit when:** egress cache-box has produced its first experimental results, OR we want to prototype metastability research ahead of schedule.

---

## A15 — `TargetLoad` and `Window` on shared `resource.Config` are dead fields for IO and Disk

**Location:** `internal/fault/resource/config.go:24,30`; consumed by `cpu/`, `memory/`. Ignored by `io/`, `disk/`.

**Decision:** Kept `TargetLoad` and `Window` on the shared `resource.Config` base. IO and Disk inherit them via struct embedding but never read them; their pressure is configured through explicit rate knobs (`ReadRate`, `WriteRate` in bytes/sec).

**Reasoning at the time:**
- IO and Disk are flow-shaped resources where bytes/sec is the natural intervention unit. `TargetLoad × capacity` doesn't map cleanly to disk bandwidth without per-host benchmarking, and the canonical fault-injection question is "add N MB/s of pressure," not "drive disk to 50% utilisation."
- Moving `TargetLoad`/`Window` off the shared base onto per-fault configs (`cpu.Config`, `memory.Config`) is the cleaner shape but touches every wire-decode path in `compiled_rule.go`. Out of scope for the incremental-targeting fix that landed alongside this note.

**Why it's worth revisiting:**
- Dead knobs are a foot-gun: `target_load: 0.5` on a disk-stress rule is silently ignored. The type system claims a uniformity that doesn't hold.
- Only the faults that consume a field should declare it.
- Pairs naturally with A16 (compiled-rule wire format for resource fault config).

**Revisit when:** `compiled_rule.go` decoders are touched for any reason, or when adding a new resource fault that has yet another shape (e.g., GPU, network bandwidth as a resource).

---

## A16 — Compiled-rule wire format for resource faults is flat, not nested per-fault config

**Location:** `compiled_rule.go` (`decodeResourceFault`)

**Decision:** Rule JSON has resource-fault parameters at the top level of the resource decision (e.g., `{"type": "memory", "target_load": 0.02, "duration_ms": 30000, ...}`) rather than under a per-fault config object (`{"type": "memory", "config": {"target_load": 0.02, ...}}`).

**Reasoning at the time:**
- Current decoders work; changing the wire shape requires coordinated changes in manteion-go's `ruleconv`, the `FaultRequest` struct, and any stored rule blobs in Postgres.
- The nested `FaultRequest{Category, Type, DurationMs, Config json.RawMessage}` shape is part of a separate cleanup (V5 long-running-fault plan, fix H). Cheaper to do both at once when the time comes.

**Why it's worth revisiting:**
- A nested config object aligns with the per-fault config types (`memory.Config`, `cpu.Config` if split per A15) and makes per-type schema validation cleaner — Manteion can validate `Config` payloads against per-type schemas before fanout.
- Rule serialization is friendlier to consumers (UI, audit log) when each fault type owns its own config schema.

**Revisit when:** the V5 long-running-fault plan's `FaultRequest` cleanup ships, or when Manteion adds per-type schema validation at the API layer.

---

## Summary: what this list is *not*

- Not a bug list — every item above compiles, tests pass, race detector clean.
- Not a TODO list — these are judgment calls, not unfinished work.
- Not a style guide — aesthetic preferences are only noted where they intersect with maintainability.

When picking items off this list, the right flow is: revisit the context, decide whether the reasoning still holds, then either (a) close the item as "current decision is correct, keeping as-is," (b) implement the alternative, or (c) promote it to a real TODO with an owner.
