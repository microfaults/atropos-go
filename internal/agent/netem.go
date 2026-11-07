// Netem faults are applied by tc netem on the target pod's egress interface,
// inside the pod's network namespace (the agent enters it through a process
// of the pod's cgroup). They shape egress only: ingress shaping needs an ifb
// device, which the agent does not set up. The envelope's scope multiplies a
// packet_loss percentage (scope 0.5 of a 10% loss drops 5% of the target's
// packets); latency has no per-packet fraction, so it requires scope 1. Each
// fault installs one plan (a root qdisc, plus a prio band and filters when it
// is targeted) that its stop removes as a whole.

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// NetemSpec is a validated netem fault. Target is the envelope's host[:port]
// verbatim ("" means the whole pod's egress). Loss and Correlation are
// percentages (packet_loss, Loss already multiplied by the scope); Delay and
// Jitter apply to latency.
type NetemSpec struct {
	FaultType         string
	Target            string
	Loss, Correlation float64
	Delay, Jitter     time.Duration
}

// NetemSpecFromRequest validates a network fault request for tc netem and
// turns it into a NetemSpec. It is the one validation the sidecar runs at
// decode time and the agent at start time. Checks, in order: the category is
// network; the envelope is present; the direction is "" or "upstream"
// (egress only); the scope (0 means 1); then the params of packet_loss or
// latency (latency needs mechanism netem and scope 1); any other fault type
// is rejected.
func NetemSpecFromRequest(req wire.FaultRequest) (NetemSpec, error) {
	if req.Category != "network" {
		return NetemSpec{}, fmt.Errorf("netem: category %q is not network", req.Category)
	}
	if req.Network == nil {
		return NetemSpec{}, errors.New("netem: a network fault needs its network envelope")
	}
	switch req.Network.Direction {
	case "", "upstream":
	default:
		return NetemSpec{}, fmt.Errorf("netem: direction %q unsupported: netem shapes egress only (ingress would need an ifb device)", req.Network.Direction)
	}
	scope := req.Network.Scope
	if scope == 0 {
		scope = 1
	}
	params := req.Params
	if len(params) == 0 {
		params = json.RawMessage(`{}`)
	}
	spec := NetemSpec{FaultType: req.FaultType, Target: req.Network.Target}
	switch req.FaultType {
	case "packet_loss":
		var p faultparams.NetworkPacketLoss
		if err := json.Unmarshal(params, &p); err != nil {
			return NetemSpec{}, fmt.Errorf("netem: packet_loss params: %w", err)
		}
		if err := p.Validate(); err != nil {
			return NetemSpec{}, fmt.Errorf("netem: %w", err)
		}
		spec.Loss = p.Loss * scope
		spec.Correlation = p.Correlation
	case "latency":
		var p faultparams.NetworkLatency
		if err := json.Unmarshal(params, &p); err != nil {
			return NetemSpec{}, fmt.Errorf("netem: latency params: %w", err)
		}
		if err := p.Validate(); err != nil {
			return NetemSpec{}, fmt.Errorf("netem: %w", err)
		}
		if p.Mechanism != faultparams.MechanismNetem {
			return NetemSpec{}, fmt.Errorf("netem: latency with mechanism %q is a proxy toxic, not netem", p.Mechanism)
		}
		if scope != 1 {
			return NetemSpec{}, fmt.Errorf("netem: latency needs scope 1, got scope %v (netem delays every packet of the target)", scope)
		}
		// Validate has parsed both durations already.
		spec.Delay, _ = time.ParseDuration(p.Delay)
		if p.Jitter != "" {
			spec.Jitter, _ = time.ParseDuration(p.Jitter)
		}
	default:
		return NetemSpec{}, fmt.Errorf("netem: fault type %q: netem supports packet_loss and latency only", req.FaultType)
	}
	return spec, nil
}

// netemArgs is the netem qdisc's arguments for spec: "loss <loss>% [<corr>%]"
// or "delay <delay>ms [<jitter>ms]". The optional value is omitted when 0;
// numbers use the shortest exact decimal and durations are rendered in
// milliseconds (1500µs is "1.5ms").
func netemArgs(spec NetemSpec) []string {
	if spec.FaultType == "latency" {
		args := []string{"delay", netemMs(spec.Delay)}
		if spec.Jitter != 0 {
			args = append(args, netemMs(spec.Jitter))
		}
		return args
	}
	args := []string{"loss", netemPct(spec.Loss)}
	if spec.Correlation != 0 {
		args = append(args, netemPct(spec.Correlation))
	}
	return args
}

func netemPct(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) + "%" }

func netemMs(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/1e6, 'f', -1, 64) + "ms"
}

// netemPlan is the ordered tc argv lines (without nsenter) that install spec
// on dev. Without ips the netem qdisc is the root qdisc and shapes the whole
// pod's egress. With ips the root is a 4-band prio qdisc whose priomap never
// names band 4, netem hangs off band 4 (1:4, handle 40:), and one u32 filter
// per ip (plus a dport match when port > 0) steers matching packets into it,
// so unmatched traffic is untouched.
func netemPlan(dev string, ips []net.IP, port int, spec NetemSpec) [][]string {
	args := netemArgs(spec)
	if len(ips) == 0 {
		return [][]string{append([]string{"tc", "qdisc", "add", "dev", dev, "root", "handle", "1:", "netem"}, args...)}
	}
	plan := [][]string{
		{"tc", "qdisc", "add", "dev", dev, "root", "handle", "1:", "prio", "bands", "4",
			"priomap", "1", "2", "2", "2", "1", "2", "0", "0", "1", "1", "1", "1", "1", "1", "1", "1"},
		append([]string{"tc", "qdisc", "add", "dev", dev, "parent", "1:4", "handle", "40:", "netem"}, args...),
	}
	for _, ip := range ips {
		line := []string{"tc", "filter", "add", "dev", dev, "parent", "1:", "protocol", "ip", "prio", "1",
			"u32", "match", "ip", "dst", ip.String() + "/32"}
		if port > 0 {
			line = append(line, "match", "ip", "dport", strconv.Itoa(port), "0xffff")
		}
		plan = append(plan, append(line, "flowid", "1:4"))
	}
	return plan
}

// pickInterface picks the pod's egress interface from `ip -json link show`
// output: the first ether link that is UP and not LOOPBACK, "" when none or
// when the listing does not parse.
func pickInterface(ipLinkJSON []byte) string {
	var links []struct {
		IfName   string   `json:"ifname"`
		Flags    []string `json:"flags"`
		LinkType string   `json:"link_type"`
	}
	if err := json.Unmarshal(ipLinkJSON, &links); err != nil {
		return ""
	}
	for _, l := range links {
		if l.LinkType != "ether" {
			continue
		}
		up, loopback := false, false
		for _, f := range l.Flags {
			switch f {
			case "UP":
				up = true
			case "LOOPBACK":
				loopback = true
			}
		}
		if up && !loopback {
			return l.IfName
		}
	}
	return ""
}
