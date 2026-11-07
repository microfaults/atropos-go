// Package agent is the node agent (atropos-agent) that executes resource
// faults for sidecar-instrumented pods inside the target container's cgroup
// and netem network faults inside the target pod's network namespace
// (design §5.14), and the client the sidecar delegates through.
//
// The agent has no manteion identity: the sidecar is the SDK instance,
// receives the rule, owns the fault span and the registry entry, and calls
// the agent over a host Unix socket. A fault lives exactly as long as the
// streaming request that started it: when the sidecar stops the fault (or
// dies), the stream ends and the agent stops the burn or restores the
// quota or the netem qdisc.
package agent

import (
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// DefaultSocket is where the agent listens and sidecars connect (a hostPath
// shared with every sidecar on the node).
const DefaultSocket = "/var/run/atropos/agent.sock"

// StartRequest is the body of POST /v1/faults.
type StartRequest struct {
	PodUID    string            `json:"pod_uid"`
	Container string            `json:"container"`
	Name      string            `json:"name"` // rule or slot name, for diagnostics
	Fault     wire.FaultRequest `json:"fault"`
}

// StartResponse is the first NDJSON line of the streaming response.
type StartResponse struct {
	ID string `json:"id"`
}

// Result is the final NDJSON line: the fault's outcome, mirrored onto the
// sidecar's fault span.
type Result struct {
	ActualDurationMs int64  `json:"actual_duration_ms"`
	Error            string `json:"error,omitempty"`
	Detail           any    `json:"detail,omitempty"`
}

// Status describes a running fault (GET /v1/faults).
type Status struct {
	ID        string    `json:"id"`
	PodUID    string    `json:"pod_uid"`
	Container string    `json:"container"`
	Name      string    `json:"name"`
	Category  string    `json:"category"`
	FaultType string    `json:"fault_type"`
	Mechanism string    `json:"mechanism"`
	CgroupDir string    `json:"cgroup_dir"`
	Netns     string    `json:"netns,omitempty"` // netem only: the namespace path the agent entered
	Dev       string    `json:"dev,omitempty"`   // netem only: the interface carrying the qdisc
	StartedAt time.Time `json:"started_at"`
}

// CapabilitiesResponse is the body of GET /v1/capabilities: the mechanisms
// this agent executes, in a fixed order ("burn", "quota", then "netem" when
// tc and nsenter are available).
type CapabilitiesResponse struct {
	Mechanisms []string `json:"mechanisms"`
}

// Capacity is the node's capacity, the fallback when a cgroup's limit is
// "max".
type Capacity struct {
	CPUs        float64
	MemoryBytes uint64
}
