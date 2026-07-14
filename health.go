package atropos

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// HealthStatus reports the SDK's readiness for service health checks.
type HealthStatus struct {
	Status               string    `json:"status"`
	RuleVersion          uint64    `json:"rule_version"`
	RuleCount            int       `json:"rule_count"`
	ManteionURL          string    `json:"manteion_url,omitempty"`
	LastSuccessfulPollAt time.Time `json:"last_successful_poll_at,omitempty"`
	// StaleFor is the human-readable duration since LastSuccessfulPollAt.
	// Empty if never polled. Helps ops dashboards distinguish a 5s vs 5h DEGRADED.
	StaleFor string `json:"stale_for,omitempty"`
}

// globalClient is set by connectManteion so Health() and the health handler
// work without threading the client everywhere.
var globalClient atomic.Pointer[manteionClient]

// setGlobalClient stores c as the package-level client.
// Called internally by connectManteion.
func setGlobalClient(c *manteionClient) {
	globalClient.Store(c)
}

// Health returns the current SDK health status.
// With no control plane configured (offline mode), Status is "offline".
func Health() HealthStatus {
	c := globalClient.Load()
	return healthFrom(c)
}

// healthHandler reports SDK health as JSON (mounted at /atropos/health).
func healthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := Health()
		code := http.StatusOK
		if h.Status == "disconnected" {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(h)
	})
}

func healthFrom(c *manteionClient) HealthStatus {
	if c == nil {
		return HealthStatus{Status: "offline"}
	}

	status := manteionStatus(c.status.Load())
	statusStr := "disconnected"
	switch status {
	case manteionConnected:
		statusStr = "connected"
	case manteionDegraded:
		statusStr = "degraded"
	}

	h := HealthStatus{
		Status:      statusStr,
		RuleVersion: c.ruleVersion.Load(),
		ManteionURL: c.cfg.url,
	}

	if c.targets.Evaluator != nil {
		h.RuleCount = len(c.targets.Evaluator.Rules())
	}

	if nanos := c.lastPollAt.Load(); nanos != 0 {
		h.LastSuccessfulPollAt = time.Unix(0, nanos)
		if status == manteionDegraded {
			h.StaleFor = time.Since(h.LastSuccessfulPollAt).Truncate(time.Second).String()
		}
	}

	return h
}
