package atropos

// swaggerFaultAdminPost documents POST /admin/fault.
//
// @Summary      Activate a runtime fault
// @Description  Installs a single demo/admin fault decision. Inline faults run in request flow; network and resource faults run in the background.
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        fault  body      FaultRequest   true  "fault request"
// @Success      201    {object}  FaultStatus
// @Failure      400    {object}  ErrorResponse  "invalid fault request"
// @Failure      405    {object}  ErrorResponse  "method not allowed"
// @Router       /admin/fault [post]
func swaggerFaultAdminPost() {}

// swaggerFaultAdminGet documents GET /admin/fault.
//
// @Summary  Inspect active runtime fault
// @Tags     admin
// @Produce  json
// @Success  200  {object}  FaultStatus
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/fault [get]
func swaggerFaultAdminGet() {}

// swaggerFaultAdminDelete documents DELETE /admin/fault.
//
// @Summary  Clear active runtime fault
// @Tags     admin
// @Produce  json
// @Success  200  {object}  FaultStatus
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/fault [delete]
func swaggerFaultAdminDelete() {}

// swaggerCacheBoxAdminGet documents GET /admin/cachebox.
//
// @Summary  Inspect cache-box state
// @Description  Store, recorder and record-buffer counters, plus the replay_with_delay source in force: whether a fitted source is installed, its mu and sigma, and the (experiment_id, phase_id) it is scoped to (both empty = every replay).
// @Tags     admin
// @Produce  json
// @Success  200  {object}  CacheBoxStats
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/cachebox [get]
func swaggerCacheBoxAdminGet() {}

// swaggerCacheBoxDelayPost documents POST /admin/cachebox/delay.
//
// @Summary  Configure cache-box replay delay
// @Description  Installs a lognormal delay source for replay_with_delay. A context scopes it to its (experiment_id, phase_id): a replay under any other pair keeps sleeping the entry's observed latency, and without a context the source applies to every replay. A source installed here lasts until the next desired-state 200 answer without freeze_cfg clears it; a 304 leaves it.
// @Tags     admin
// @Accept   json
// @Produce  json
// @Param    delay  body  DelayRequest  true  "delay distribution"
// @Success  204    "delay source configured"
// @Failure  400    {object}  ErrorResponse  "malformed JSON, negative mu or sigma, or a context naming only one of experiment_id and phase_id"
// @Failure  405    {object}  ErrorResponse  "method not allowed"
// @Router   /admin/cachebox/delay [post]
func swaggerCacheBoxDelayPost() {}

// swaggerCacheBoxAdminDelete documents DELETE /admin/cachebox.
//
// @Summary  Clear cache-box entries
// @Description  Clears the record-side store and the installed replay set.
// @Tags     admin
// @Produce  json
// @Success  204  "cache cleared"
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/cachebox [delete]
func swaggerCacheBoxAdminDelete() {}

// swaggerPreloadBeginPost documents POST /cachebox/preload/begin.
//
// @Summary      Start a staged cache-box preload
// @Description  Clears any prior staging for (experiment_id, phase_id) and starts a new one.
// @Tags         cachebox
// @Accept       json
// @Produce      json
// @Param        begin  body      PreloadBeginRequest   true  "preload begin"
// @Success      200    {object}  PreloadBeginResponse
// @Failure      400    {object}  ErrorResponse  "invalid request"
// @Failure      409    {object}  ErrorResponse  "unsupported key_strategy"
// @Failure      413    {object}  ErrorResponse  "too_large"
// @Router       /cachebox/preload/begin [post]
func swaggerPreloadBeginPost() {}

// swaggerPreloadChunkPost documents POST /cachebox/preload/chunk.
//
// @Summary      Append a chunk to a staged cache-box preload
// @Description  Idempotent by chunk_seq -- redelivering the same chunk_seq does not double-count.
// @Tags         cachebox
// @Accept       json
// @Produce      json
// @Param        chunk  body      PreloadChunkRequest   true  "preload chunk"
// @Success      200    {object}  PreloadChunkResponse
// @Failure      400    {object}  ErrorResponse  "invalid request"
// @Failure      409    {object}  ErrorResponse  "no active preload for this pair"
// @Failure      413    {object}  ErrorResponse  "too_large"
// @Router       /cachebox/preload/chunk [post]
func swaggerPreloadChunkPost() {}

// swaggerPreloadCommitPost documents POST /cachebox/preload/commit.
//
// @Summary      Commit a staged cache-box preload
// @Description  Verifies count+checksum (wire spec §W5); on match, atomically installs the staged set as the replay set for the pair. On mismatch, staging is dropped and any previously installed set is left untouched.
// @Tags         cachebox
// @Accept       json
// @Produce      json
// @Param        commit  body      PreloadCommitRequest   true  "preload commit"
// @Success      200     {object}  PreloadCommitResponse  "checksum matched, swap happened"
// @Failure      400     {object}  ErrorResponse  "invalid request"
// @Failure      409     {object}  PreloadCommitResponse  "checksum mismatch, no swap"
// @Router       /cachebox/preload/commit [post]
func swaggerPreloadCommitPost() {}

// swaggerPreloadAbortPost documents POST /cachebox/preload/abort.
//
// @Summary  Abort a staged cache-box preload
// @Tags     cachebox
// @Accept   json
// @Param    abort  body  PreloadAbortRequest  true  "preload abort"
// @Success  200    "staging dropped"
// @Failure  400    {object}  ErrorResponse  "invalid request"
// @Router   /cachebox/preload/abort [post]
func swaggerPreloadAbortPost() {}

// swaggerCacheBoxFidelityGet documents GET /cachebox/fidelity.
//
// @Summary      Read per-(experiment, phase) cache-box fidelity counters
// @Description  Synchronous, pull-based snapshot of everything needed to compute a phase's VALID/INVALID verdict from this instance (design doc Q6).
// @Tags         cachebox
// @Produce      json
// @Param        experiment_id  query     string  true  "experiment id"
// @Param        phase_id       query     string  true  "phase id"
// @Success      200            {object}  FidelitySnapshot
// @Failure      400            {object}  ErrorResponse  "missing experiment_id or phase_id"
// @Failure      405            {object}  ErrorResponse  "method not allowed"
// @Router       /cachebox/fidelity [get]
func swaggerCacheBoxFidelityGet() {}

// swaggerRulesAdminGet documents GET /admin/rules.
//
// @Summary  List runtime rules
// @Tags     admin
// @Produce  json
// @Success  200  {array}  StaticRule
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/rules [get]
func swaggerRulesAdminGet() {}

// swaggerRulesAdminPost documents POST /admin/rules.
//
// @Summary  Replace runtime rules
// @Description  Applies the rule set through the single apply path the manteion sync also uses, scoped to rules: admin fault slots, the delay source and the installed replay set stay as they are. Rules this host cannot execute (a category outside its capabilities, quota resource faults in-process, network host=inline outside the sidecar or host=proxy without a resolver, compositions) are skipped per rule, logged and counted in atropos_rules_unsupported_total{category}; the rest apply and the answer is 204. A rule that does not decode is skipped too, but the answer is 422. The pushed set holds until the next desired-state 200 answer replaces it.
// @Tags     admin
// @Accept   json
// @Produce  json
// @Param    rules  body  []CompiledRule  true  "replacement rules (null clears the set)"
// @Success  204    "rules replaced (rules this host cannot execute are skipped)"
// @Failure  400    {object}  ErrorResponse  "malformed JSON"
// @Failure  405    {object}  ErrorResponse  "method not allowed"
// @Failure  422    {object}  ErrorResponse  "some rules did not decode and were skipped, the rest were applied; error joins the per-rule messages with '; '"
// @Failure  500    {object}  ErrorResponse  "hard error: this host cannot hold rules, nothing was applied"
// @Router   /admin/rules [post]
func swaggerRulesAdminPost() {}

// swaggerHealthGet documents GET /health.
//
// @Summary  Report SDK health
// @Tags     health
// @Produce  json
// @Success  200  {object}  HealthStatus
// @Router   /health [get]
func swaggerHealthGet() {}

// swaggerSDKRulesGet documents GET /api/v1/sdk/rules, the desired-state poll.
// manteion serves it and the SDK calls it (desired-state protocol v1, §2); it
// is listed so the wire types the poll carries are part of this contract.
//
// @Summary      Poll the desired state (served by manteion)
// @Description  Served by manteion, not by Serve: the SDK is the client. version is the generation the instance holds, If-None-Match the ETag of the document it holds (sent only when it holds one), and wait asks manteion to hold the request until the service's generation changes or the wait (seconds) elapses (the SDK sends 30). The ETag is a content hash alone; the generation travels in X-Atropos-Generation and in the body's version. The SDK treats the ETag as opaque. 200 is applied through the single apply path and reported with POST /api/v1/sdk/status; 304 means the document is unchanged and the instance adopts the generation without applying.
// @Tags         manteion-protocol
// @Produce      json
// @Param        service        query   string  true   "service name"
// @Param        instance_id    query   string  true   "instance id"
// @Param        version        query   int     false  "generation the instance holds"
// @Param        wait           query   int     false  "long-poll hold in seconds"
// @Param        If-None-Match  header  string  false  "ETag of the document the instance holds"
// @Success      200  {object}  RuleSync  "the full desired state"
// @Header       200  {string}  ETag                    "ETag of the document"
// @Header       200  {integer} X-Atropos-Generation   "generation of the desired state"
// @Success      304  "unchanged since If-None-Match"
// @Header       304  {string}  ETag                    "ETag of the document"
// @Header       304  {integer} X-Atropos-Generation   "generation of the desired state, to adopt"
// @Router       /api/v1/sdk/rules [get]
func swaggerSDKRulesGet() {}

// swaggerSDKStatusPost documents POST /api/v1/sdk/status, the instance's
// report of what it holds. manteion serves it and the SDK calls it (desired-state
// protocol v1, §2).
//
// @Summary      Report the desired state held (served by manteion)
// @Description  Served by manteion, not by Serve: the SDK is the client. Sent after every apply and whenever the replay-set state changes. The report is a full snapshot of what the instance holds: unsupported and errors replace the previous report (an empty list clears it), and an absent replay_set means the instance holds none. Any 2xx counts as delivered (manteion answers 204); a report that fails to send is re-sent.
// @Tags         manteion-protocol
// @Accept       json
// @Param        status  body  StatusReport  true  "what the instance holds"
// @Success      204  "report stored"
// @Router       /api/v1/sdk/status [post]
func swaggerSDKStatusPost() {}
