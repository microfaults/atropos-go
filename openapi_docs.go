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

// swaggerCacheBoxEntriesPost documents POST /admin/cachebox/entries.
//
// @Summary      Preload cache-box entries
// @Description  Inserts wire-format cache entries into the cache-box store.
// @Tags         admin
// @Accept       json
// @Produce      json
// @Param        entries  body  []CacheBoxWireEntry  true  "cache entries"
// @Success      204      "entries inserted"
// @Failure      400      {object}  ErrorResponse  "invalid entries"
// @Failure      405      {object}  ErrorResponse  "method not allowed"
// @Router       /admin/cachebox/entries [post]
func swaggerCacheBoxEntriesPost() {}

// swaggerCacheBoxAdminDelete documents DELETE /admin/cachebox.
//
// @Summary  Clear cache-box entries
// @Tags     admin
// @Produce  json
// @Success  204  "cache cleared"
// @Failure  405  {object}  ErrorResponse  "method not allowed"
// @Router   /admin/cachebox [delete]
func swaggerCacheBoxAdminDelete() {}

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
