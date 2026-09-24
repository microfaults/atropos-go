package atropos

import (
	"sync/atomic"

	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
)

// globalClient is set by Serve so Health() works without threading the
// client everywhere. Cleared on shutdown.
var globalClient atomic.Pointer[instrument.ManteionClient]

// Health returns the current SDK health status.
// With no control plane configured (offline mode), Status is "offline".
func Health() HealthStatus {
	return instrument.HealthFrom(globalClient.Load())
}
