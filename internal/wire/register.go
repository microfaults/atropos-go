package wire

// RegisterRequest is the POST body for /api/v1/sdk/register.
type RegisterRequest struct {
	ID             string `json:"id"`
	Service        string `json:"service"`
	Version        string `json:"version,omitempty"`
	Address        string `json:"address"`
	PollIntervalMs int64  `json:"poll_interval_ms,omitempty"` // how often this instance's polls come back: the longer of its poll interval and its long-poll hold (30s by default); manteion uses it for liveness detection
	// Routes is the HTTP route inventory for manteion's workflow-builder
	// catalog. Optional — gRPC services register without routes.
	Routes []Route `json:"routes,omitempty"`
	// Host names the instrument host: "in-process" (the Serve embed) or
	// "sidecar" (atroposd). Additive; absent from older SDKs.
	Host string `json:"host,omitempty"`
	// Capabilities lists the fault categories this host can execute
	// ("inline", "network", "resource", "cachebox"), so the control plane
	// can filter delivery (design §5.3, M3). Additive; absent = all.
	Capabilities []string `json:"capabilities,omitempty"`
} //@name atropos.RegisterRequest

// RegisterResponse is the JSON manteion returns from /api/v1/sdk/register:
// a status string plus the same RuleSync desired-state payload the poll
// endpoint serves, so the SDK converges before its first poll — e.g. during
// a rolling deploy while an experiment is in progress.
type RegisterResponse struct {
	Status string `json:"status"`
	RuleSync
} //@name atropos.RegisterResponse

// Route is one HTTP route published to manteion at registration time.
// Manteion aggregates routes across all live SDK instances into the
// workflow-builder catalog, so the endpoint picker reflects what is
// actually deployed on the cluster.
//
// Path is the literal template the service serves (e.g. "/product/{id}");
// placeholder substitution happens at load-generation time. DependsOn names
// prerequisite routes as "METHOD /path" (same service) or
// "service METHOD /path" (fully qualified); manteion drops unresolvable
// references at catalog-build time.
type Route struct {
	Method      string   `json:"method"`
	Path        string   `json:"path"`
	Description string   `json:"description,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
} //@name atropos.Route
