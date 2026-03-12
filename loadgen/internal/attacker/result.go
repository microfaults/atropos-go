package attacker

import (
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

// AttackResult aggregates vegeta metrics for a completed attack.
type AttackResult struct {
	TotalRequests uint64            `json:"total_requests"`
	Duration      time.Duration     `json:"duration"`
	RateActual    float64           `json:"rate_actual"`
	Success       float64           `json:"success"`
	StatusCodes   map[string]int    `json:"status_codes"`
	Latencies     LatencySummary    `json:"latencies"`
	BytesIn       BytesSummary      `json:"bytes_in"`
	BytesOut      BytesSummary      `json:"bytes_out"`
	Errors        []string          `json:"errors,omitempty"`
}

// LatencySummary holds latency percentiles.
type LatencySummary struct {
	P50 time.Duration `json:"p50"`
	P90 time.Duration `json:"p90"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
	Max time.Duration `json:"max"`
	Min time.Duration `json:"min"`
}

// BytesSummary holds byte transfer stats.
type BytesSummary struct {
	Total uint64  `json:"total"`
	Mean  float64 `json:"mean"`
}

// FromMetrics converts vegeta.Metrics into an AttackResult.
func FromMetrics(m *vegeta.Metrics) *AttackResult {
	codes := make(map[string]int, len(m.StatusCodes))
	for k, v := range m.StatusCodes {
		codes[k] = v
	}

	errs := make([]string, len(m.Errors))
	copy(errs, m.Errors)

	return &AttackResult{
		TotalRequests: m.Requests,
		Duration:      m.Duration,
		RateActual:    m.Rate,
		Success:       m.Success,
		StatusCodes:   codes,
		Latencies: LatencySummary{
			P50: m.Latencies.P50,
			P90: m.Latencies.P90,
			P95: m.Latencies.P95,
			P99: m.Latencies.P99,
			Max: m.Latencies.Max,
			Min: m.Latencies.Min,
		},
		BytesIn: BytesSummary{
			Total: m.BytesIn.Total,
			Mean:  m.BytesIn.Mean,
		},
		BytesOut: BytesSummary{
			Total: m.BytesOut.Total,
			Mean:  m.BytesOut.Mean,
		},
		Errors: errs,
	}
}
