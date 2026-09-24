package wire

import "time"

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
	// Host and Capture say which instrument host answered ("in-process" |
	// "sidecar") and, for the sidecar, how traffic reaches it ("explicit" |
	// "transparent").
	Host    string `json:"host,omitempty"`
	Capture string `json:"capture,omitempty"`
} //@name atropos.HealthStatus
