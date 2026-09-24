package instrument

import (
	"encoding/json"
	"net/http"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// HealthHandler reports SDK health as JSON (mounted at /atropos/health).
// c is the connected control-plane client, or nil in offline mode; host and
// capture are stamped on every answer so an operator can tell which
// instrument host is answering and how traffic reaches it.
func HealthHandler(c *ManteionClient, host, capture string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := healthFrom(c)
		h.Host, h.Capture = host, capture
		code := http.StatusOK
		if h.Status == "disconnected" {
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(h)
	})
}

// HealthFrom is healthFrom for hosts outside the package (atropos.Health).
func HealthFrom(c *ManteionClient) wire.HealthStatus { return healthFrom(c) }

func healthFrom(c *ManteionClient) wire.HealthStatus {
	if c == nil {
		return wire.HealthStatus{Status: "offline"}
	}

	status := manteionStatus(c.status.Load())
	statusStr := "disconnected"
	switch status {
	case manteionConnected:
		statusStr = "connected"
	case manteionDegraded:
		statusStr = "degraded"
	}

	h := wire.HealthStatus{
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
