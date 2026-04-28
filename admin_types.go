package atropos

import "atropos-go/internal/cachebox"

// FaultRequest is the JSON body for POST /admin/fault.
// Exported so manteion-go's admin client can use it as a typed request.
type FaultRequest = faultRequest

// FaultStatus is the JSON response for GET /admin/fault.
// Exported so manteion-go's admin client can decode the response.
type FaultStatus = faultStatus

// DelayRequest is the JSON body for POST /admin/cachebox/delay.
type DelayRequest struct {
	Mu    float64 `json:"mu"`
	Sigma float64 `json:"sigma"`
	Seed  uint64  `json:"seed"`
}

// CacheBoxStats is the JSON response for GET /admin/cachebox.
type CacheBoxStats = cachebox.Stats
