package atropos

import "sync"

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
}

var (
	routesMu         sync.RWMutex
	registeredRoutes []Route
)

// RegisterRoutes records the service's HTTP route inventory for publication
// to manteion. Call it at startup, before ConnectManteion, so the inventory
// rides the initial register payload. Calling it after ConnectManteion also
// works: the SDK fires a best-effort background re-register (idempotent on
// the instance ID) so the catalog converges without waiting for a reconnect.
//
// Each call replaces the previous inventory. gRPC-only services simply never
// call this — registering without routes is valid.
func RegisterRoutes(routes ...Route) {
	routesMu.Lock()
	registeredRoutes = append([]Route(nil), routes...)
	routesMu.Unlock()

	if c := globalClient.Load(); c != nil && c.pollCtx != nil {
		go func() {
			if err := c.register(c.pollCtx); err != nil {
				c.logger.Warn("re-register after RegisterRoutes failed; routes publish on next re-register",
					"error", err)
			}
		}()
	}
}

// publishedRoutes returns a copy of the current route inventory, or nil when
// none is registered (so the register payload omits the field entirely).
func publishedRoutes() []Route {
	routesMu.RLock()
	defer routesMu.RUnlock()
	if len(registeredRoutes) == 0 {
		return nil
	}
	return append([]Route(nil), registeredRoutes...)
}
