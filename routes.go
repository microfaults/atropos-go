package atropos

// Route is one HTTP route published to manteion at registration time.
// Manteion aggregates routes across all live SDK instances into the
// workflow-builder catalog, so the endpoint picker reflects what is
// actually deployed on the cluster. Supply routes via Config.Routes; they
// ride the register payload (and every idempotent re-register).
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
}
