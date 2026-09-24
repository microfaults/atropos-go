// types.go re-exports, as transparent type aliases, every wire shape the
// root package has always exported. Declarations live in internal/wire
// (and internal/cachebox, internal/evaluator, internal/interceptor); the
// names, fields and json tags here are pinned by the cross-repo wire tests
// and must not drift: manteion imports them as atroposdk.X.
package atropos

import (
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// Interceptor ties the evaluator, fault execution, and OTel together. It is
// exported only as the currency between DefaultInterceptor and the grpc
// subpackage's interceptor constructors.
type Interceptor = interceptor.Interceptor

// NetworkResolver resolves a target into listen and upstream addresses for
// network fault proxies. Supplied via Config.NetworkResolver.
type NetworkResolver = wire.NetworkResolver

// Faults and rules.
type (
	FaultRequest              = wire.FaultRequest
	FaultStatus               = wire.FaultStatus
	NetworkEnvelope           = wire.NetworkEnvelope
	CompiledRule              = wire.CompiledRule
	CompiledCacheBox          = wire.CompiledCacheBox
	CompiledFault             = wire.CompiledFault
	CompiledComposition       = wire.CompiledComposition
	CompiledCompositionMember = wire.CompiledCompositionMember
	RuleSync                  = wire.RuleSync
	StaticRule                = wire.StaticRule
)

// Registration.
type (
	RegisterRequest  = wire.RegisterRequest
	RegisterResponse = wire.RegisterResponse
	Route            = wire.Route
)

// Cache-box control, preload (W4/W5), drain (W3), fidelity (W6).
type (
	CacheBoxContext       = wire.CacheBoxContext
	CacheBoxWireEntry     = wire.CacheBoxWireEntry
	CacheBoxStats         = wire.CacheBoxStats
	DelayRequest          = wire.DelayRequest
	DrainReport           = wire.DrainReport
	DrainReportResponse   = wire.DrainReportResponse
	PreloadBeginRequest   = wire.PreloadBeginRequest
	PreloadBeginResponse  = wire.PreloadBeginResponse
	PreloadChunkRequest   = wire.PreloadChunkRequest
	PreloadChunkResponse  = wire.PreloadChunkResponse
	PreloadCommitRequest  = wire.PreloadCommitRequest
	PreloadCommitResponse = wire.PreloadCommitResponse
	PreloadAbortRequest   = wire.PreloadAbortRequest
	FidelityMissReasons   = wire.FidelityMissReasons
	FidelityReplayAge     = wire.FidelityReplayAge
	FidelityPreloadState  = wire.FidelityPreloadState
	FidelitySnapshot      = wire.FidelitySnapshot
)

// Health and errors.
type (
	HealthStatus  = wire.HealthStatus
	ErrorResponse = wire.ErrorResponse
)
