package proxy

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// Policy is one armed inline network toxic: which upstream it applies to,
// which direction of the proxied exchange it shapes, what fraction of the
// matching requests it affects, and the toxic itself.
type Policy struct {
	Name      string            // rule or slot name; the fault-registry key
	Target    string            // "host:port" or "host", matched against the outbound URL host
	Direction network.Direction // Downstream shapes response bytes, Upstream request bytes
	Scope     float64           // fraction of matching requests affected; 0 → 1.0
	Toxic     network.Toxic
}

type armed struct {
	Policy
	ctx      context.Context
	matched  atomic.Int64
	affected atomic.Int64
}

// PolicyTable holds the armed policies of one sidecar. ToxicTransport
// consults it per request; PolicyFault arms and disarms.
type PolicyTable struct {
	mu    sync.RWMutex
	armed map[string]*armed
	roll  func() float64
}

// NewPolicyTable returns an empty table.
func NewPolicyTable() *PolicyTable {
	return &PolicyTable{armed: map[string]*armed{}, roll: rand.Float64}
}

func (t *PolicyTable) arm(ctx context.Context, p Policy) *armed {
	a := &armed{Policy: p, ctx: ctx}
	t.mu.Lock()
	t.armed[p.Name] = a
	t.mu.Unlock()
	return a
}

// disarm removes a only if it is still the armed instance under its name:
// a re-armed rule must not be disarmed by its predecessor's expiry.
func (t *PolicyTable) disarm(a *armed) {
	t.mu.Lock()
	if t.armed[a.Name] == a {
		delete(t.armed, a.Name)
	}
	t.mu.Unlock()
}

// Armed reports whether a policy is armed under name.
func (t *PolicyTable) Armed(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, ok := t.armed[name]
	return ok
}

// match returns the armed policy that applies to a request bound for host
// ("host:port"), after rolling its scope. Policies are consulted in name
// order so the choice is stable; the first target match wins whether or
// not the roll selects it, and a matched-but-not-affected request is
// counted as such.
func (t *PolicyTable) match(host string) (*armed, bool) {
	t.mu.RLock()
	names := make([]string, 0, len(t.armed))
	for n := range t.armed {
		names = append(names, n)
	}
	sort.Strings(names)
	var hit *armed
	for _, n := range names {
		if targetMatches(t.armed[n].Target, host) {
			hit = t.armed[n]
			break
		}
	}
	t.mu.RUnlock()
	if hit == nil {
		return nil, false
	}
	hit.matched.Add(1)
	scope := hit.Scope
	if scope <= 0 {
		scope = 1
	}
	if scope < 1 && t.roll() >= scope {
		return nil, false
	}
	hit.affected.Add(1)
	return hit, true
}

// targetMatches: an exact "host:port" match, or a port-less target matching
// the request's hostname.
func targetMatches(target, host string) bool {
	if target == host {
		return true
	}
	if _, _, err := net.SplitHostPort(target); err == nil {
		return false // target names a port; only the exact pair matches
	}
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		h = host
	}
	return target == h
}

// PolicyDetail is the Result.Detail of a completed PolicyFault.
type PolicyDetail struct {
	Matched  int64 // requests bound for the target while armed
	Affected int64 // of those, requests the scope roll selected
}

// PolicyFault is the fault.Fault that hosts an inline network toxic in the
// sidecar: Start arms the policy for the fault's duration (or until Stop),
// and every proxied request bound for the target is shaped while it is
// armed. It runs through the interceptor's registry like any background
// fault, so rule removal disarms it and a toxic never outlives its phase.
type PolicyFault struct {
	fault.FaultConfig
	Table  *PolicyTable
	Policy Policy
}

// Validate implements fault.Fault.
func (f *PolicyFault) Validate() error {
	if err := f.FaultConfig.Validate(); err != nil {
		return err
	}
	if f.Table == nil {
		return fmt.Errorf("proxy: policy fault requires a table")
	}
	if f.Policy.Target == "" {
		return fmt.Errorf("proxy: policy fault requires network.target")
	}
	if f.Policy.Toxic == nil {
		return fmt.Errorf("proxy: policy fault requires a toxic")
	}
	return nil
}

// Start implements fault.Fault: arm now, disarm when the duration elapses
// or the handle is stopped.
func (f *PolicyFault) Start(ctx context.Context) (*fault.Handle, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, f.Duration)
	handle := fault.NewHandle(cancel)
	a := f.Table.arm(ctx, f.Policy)
	go func() {
		start := time.Now()
		<-ctx.Done()
		f.Table.disarm(a)
		handle.Send(fault.Result{
			ActualDuration: time.Since(start),
			Detail:         PolicyDetail{Matched: a.matched.Load(), Affected: a.affected.Load()},
		})
	}()
	return handle, nil
}

// InlineToxicFactory is the instrument.InlineToxicHost for a sidecar: it
// turns a decoded host=inline network fault into a PolicyFault over table.
// Direction defaults to downstream (response bytes): an upstream-direction
// stream toxic has nothing to act on for a bodiless request.
func InlineToxicFactory(table *PolicyTable) instrument.InlineToxicHost {
	return func(name string, env *wire.NetworkEnvelope, toxic network.Toxic, base fault.FaultConfig) (fault.Fault, error) {
		if env.Target == "" {
			return nil, fmt.Errorf("network fault with host=inline requires network.target")
		}
		dir := network.Downstream
		if env.Direction == "upstream" {
			dir = network.Upstream
		}
		return &PolicyFault{FaultConfig: base, Table: table, Policy: Policy{
			Name: name, Target: env.Target, Direction: dir, Scope: env.Scope, Toxic: toxic,
		}}, nil
	}
}

var _ fault.Fault = (*PolicyFault)(nil)
