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
// @Tags     admin
// @Produce  json
// @Success  200  {object}  CacheBoxStats
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/cachebox [get]
func swaggerCacheBoxAdminGet() {}

// swaggerCacheBoxDelayPost documents POST /admin/cachebox/delay.
//
// @Summary  Configure cache-box replay delay
// @Tags     admin
// @Accept   json
// @Produce  json
// @Param    delay  body  DelayRequest  true  "delay distribution"
// @Success  204    "delay source configured"
// @Failure  400    {object}  ErrorResponse  "invalid delay request"
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
// @Tags     admin
// @Accept   json
// @Produce  json
// @Param    rules  body  []StaticRule  true  "replacement rules"
// @Success  204    "rules replaced"
// @Failure  400    {object}  ErrorResponse  "invalid rules"
// @Failure  405    {object}  ErrorResponse  "method not allowed"
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
