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

// globalClient is set by ConnectManteion so the package-level Health()/Ready()
// helpers work without the caller needing to thread the client everywhere.
var globalClient atomic.Pointer[ManteionClient]

// setGlobalClient stores c as the package-level client.
// Called internally by ConnectManteion.
func setGlobalClient(c *ManteionClient) {
	globalClient.Store(c)
}

// Health returns the current SDK health status.
// If ConnectManteion returned nil (offline mode), Status is "offline".
func Health() HealthStatus {
	c := globalClient.Load()
	return healthFrom(c)
}

// Ready returns true if the service should accept traffic.
//   - connected:    yes (normal)
//   - degraded:     yes (stale rules, still functional)
//   - offline:      yes (no manteion configured, dev mode)
//   - disconnected: NO  (manteion configured but never connected)
func Ready() bool {
	h := Health()
	return h.Status != "disconnected"
}

// HealthHandler returns an http.Handler that reports SDK health as JSON.
func HealthHandler() http.Handler {
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

func healthFrom(c *ManteionClient) HealthStatus {
	if c == nil {
		return HealthStatus{Status: "offline"}
	}

	status := ManteionStatus(c.status.Load())
	statusStr := "disconnected"
	switch status {
	case ManteionConnected:
		statusStr = "connected"
	case ManteionDegraded:
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
		if status == ManteionDegraded {
			h.StaleFor = time.Since(h.LastSuccessfulPollAt).Truncate(time.Second).String()
		}
	}

	return h
}
