package instrument

import (
	"net/http"

	"git.ucsc.edu/microfaults/atropos-go/internal/metrics"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// ControlConfig configures ControlSurface.
type ControlConfig struct {
	Core *Core
	// Client is the connected control-plane client, or nil in offline mode
	// (the health endpoint then reports "offline").
	Client *ManteionClient
	// NetworkResolver is forwarded to the rule decoders behind /admin/rules
	// and /admin/fault; nil rejects network-category faults.
	NetworkResolver wire.NetworkResolver
	// Host and Capture are stamped on /atropos/health ("in-process" |
	// "sidecar"; "" | "explicit" | "transparent").
	Host    string
	Capture string
}

// ControlSurface mounts the runtime-control and observability endpoints at
// the exact paths manteion addresses:
//
//	GET  /metrics                    Prometheus metrics
//	     /atropos/health             SDK health/connectivity
//	     /admin/fault[/{id}]         arm/inspect/clear admin faults
//	     /admin/rules                inspect/replace the compiled rule set
//	     /admin/cachebox[/delay]     cache-box stats / freeze delay / clear
//	POST /cachebox/preload/*         staged replay-set preload (W4)
//	GET  /cachebox/fidelity          per-phase fidelity counters (W6)
//
// The returned mux has no "/" handler: hosts that serve business traffic
// beside the control surface add their own, wrapped in the fault/OTel
// middleware, and the control paths stay outside it.
func ControlSurface(cfg ControlConfig) *http.ServeMux {
	c := cfg.Core
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", metrics.Handler())
	mux.Handle("/atropos/health", HealthHandler(cfg.Client, cfg.Host, cfg.Capture))

	// Bare path and subtree both: the subtree pattern alone would
	// 301-redirect the bare path and drop freeze/thaw verbs.
	opts := c.decodeOptsWith(cfg.NetworkResolver)
	faultAdmin := faultAdminHandler(c.demo, c.registry, c.armFn(), opts...)
	mux.Handle("/admin/fault", faultAdmin)
	mux.Handle("/admin/fault/", faultAdmin)

	mux.Handle("/admin/rules", rulesAdminHandler(c.targets(cfg.NetworkResolver)))

	cbAdmin := cacheBoxAdminHandler(c.cacheBox)
	mux.Handle("/admin/cachebox", cbAdmin)
	mux.Handle("/admin/cachebox/", cbAdmin)
	mux.Handle("/cachebox/preload/", cacheBoxPreloadHandler(c.cacheBox))
	mux.Handle("GET /cachebox/fidelity", cacheBoxFidelityHandler(c.cacheBox, c.service, c.instance))

	return mux
}
