// types.go declares the wire-shape aliases shared with manteion. Everything
// here is either decoded from / encoded to one of the SDK's HTTP endpoints
// or embedded in the manteion sync protocol — the shapes are pinned by the
// cross-repo wire contract (wire_compat_test.go, wire_fidelity_test.go) and
// must not drift. Host-side wiring types live behind Serve.
package atropos

import (
	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
	"git.ucsc.edu/microfaults/atropos-go/internal/evaluator"
)

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies. Supplied via Config.NetworkResolver.
type NetworkResolver func(target string) (listen, upstream string, err error)

// StaticRule is the JSON shape GET /admin/rules serves (one element per
// installed rule). Manteion's SDK client decodes into it.
type StaticRule = evaluator.StaticRule

// CacheBoxWireEntry is the JSON-serializable transfer format for cache
// entries. Used for all SDK <-> manteion communication (ingest and preload).
type CacheBoxWireEntry = cachebox.WireEntry

// CacheBoxStats is the GET /admin/cachebox response: combined store,
// recorder, and record-buffer counters.
type CacheBoxStats = cachebox.Stats
