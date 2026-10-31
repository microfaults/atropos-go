package cachebox

import (
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// DelaySource returns a delay duration to sleep for before serving a cached
// entry in replay_with_delay mode. The entry is provided so sources that
// don't maintain their own state can fall back to the recorded latency.
//
// Implementations must be safe for concurrent use.
type DelaySource interface {
	Sample(entry *Entry) time.Duration
}

// ObservedDelaySource returns entry.ObservedLatency -- i.e. replay sleeps
// for exactly the latency that was recorded when the entry was captured.
// This is the MVP default: no distribution fitting, just verbatim playback.
//
// A nil entry yields 0 (no sleep).
type ObservedDelaySource struct{}

// Sample implements DelaySource.
func (ObservedDelaySource) Sample(entry *Entry) time.Duration {
	if entry == nil {
		return 0
	}
	return entry.ObservedLatency
}

// DistributionDelaySource samples replay delays from a fitted lognormal
// distribution. Manteion supplies the Mu/Sigma fit once it has collected
// enough latency samples. Until Sigma is set, Sample falls back to
// ObservedDelaySource.
//
// The lognormal form is chosen because per-request service latencies are
// empirically well-modeled by heavy-tailed distributions, and lognormal is
// a common two-parameter fit in the distributed-systems literature.
//
// It is the source a freeze installs, through SetDelaySource or
// SetScopedDelaySource: the desired state's freeze_cfg installs one (scoped to
// its context's (experiment_id, phase_id)), and POST /admin/cachebox/delay
// installs one by hand, so the delay source can change without restarting the
// SDK.
type DistributionDelaySource struct {
	mu    sync.RWMutex
	muVal float64 // lognormal mu, in ln(microseconds)
	sigma float64 // lognormal sigma
	rng   *rand.Rand
}

// NewDistributionDelaySource builds a source seeded with (mu, sigma). A
// sigma of 0 keeps the source in fallback mode. The seed is used for the
// PCG random number generator so two SDKs in the same experiment can be
// configured with different seeds to avoid synchronized delay samples.
func NewDistributionDelaySource(mu, sigma float64, seed uint64) *DistributionDelaySource {
	return &DistributionDelaySource{
		muVal: mu,
		sigma: sigma,
		rng:   rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)),
	}
}

// SetParams replaces the (mu, sigma) fit atomically.
func (d *DistributionDelaySource) SetParams(mu, sigma float64) {
	d.mu.Lock()
	d.muVal = mu
	d.sigma = sigma
	d.mu.Unlock()
}

// Params returns the current (mu, sigma) fit, read under one lock so the pair
// is never torn by a concurrent SetParams.
func (d *DistributionDelaySource) Params() (mu, sigma float64) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.muVal, d.sigma
}

// Sample draws a delay from the fitted distribution. If sigma is zero
// (distribution not yet fitted), Sample falls back to entry.ObservedLatency.
func (d *DistributionDelaySource) Sample(entry *Entry) time.Duration {
	d.mu.RLock()
	mu, sigma := d.muVal, d.sigma
	d.mu.RUnlock()

	if sigma == 0 {
		return ObservedDelaySource{}.Sample(entry)
	}

	// Box-Muller transform: produce a standard normal z then scale by (mu, sigma).
	// We hold the RLock briefly for rng access since math/rand/v2.Rand is not
	// goroutine-safe.
	d.mu.Lock()
	u1 := d.rng.Float64()
	u2 := d.rng.Float64()
	d.mu.Unlock()
	// Guard against log(0) -- u1 cannot be 0 from Float64 per the stdlib
	// contract, but defend anyway to avoid -Inf.
	if u1 <= 0 {
		u1 = 1e-12
	}
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	microseconds := math.Exp(mu + sigma*z)
	if microseconds < 0 || math.IsNaN(microseconds) || math.IsInf(microseconds, 0) {
		return ObservedDelaySource{}.Sample(entry)
	}
	return time.Duration(microseconds) * time.Microsecond
}

// DelayStats describes the replay_with_delay source a CacheBox is sampling
// from, as reported in Stats.Delay. GET /admin/cachebox serves Stats
// verbatim, so the fields carry no json tags, like their siblings there.
//
// Installed is true only for a *DistributionDelaySource, the fitted source a
// freeze installs; the default observed-latency source (and any other
// DelaySource) reads as not installed, with Mu and Sigma zero. Mu and Sigma
// are the source's current fit (Params; mu is in ln(microseconds)).
// ExperimentID and PhaseID are the scope the source is limited to; both empty
// means it applies to every replay.
type DelayStats struct {
	Installed    bool
	ExperimentID string
	PhaseID      string
	Mu           float64
	Sigma        float64
}
