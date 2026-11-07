package agent

import (
	"encoding/json"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

func netReq(faultType, target, direction string, scope float64, params string) wire.FaultRequest {
	return wire.FaultRequest{Category: "network", FaultType: faultType, DurationMs: 1000,
		Network: &wire.NetworkEnvelope{Host: "inline", Target: target, Direction: direction, Scope: scope},
		Params:  json.RawMessage(params)}
}

func TestNetemSpecFromRequest(t *testing.T) {
	spec, err := NetemSpecFromRequest(netReq("packet_loss", "productcatalogservice:3550", "upstream", 0.5, `{"loss":10,"correlation":25}`))
	if err != nil || spec.Loss != 5 || spec.Correlation != 25 || spec.Target != "productcatalogservice:3550" || spec.FaultType != "packet_loss" {
		t.Fatalf("packet_loss: %+v %v (scope must multiply loss)", spec, err)
	}
	spec, err = NetemSpecFromRequest(netReq("latency", "", "", 0, `{"delay":"100ms","jitter":"20ms","mechanism":"netem"}`))
	if err != nil || spec.Delay != 100*time.Millisecond || spec.Jitter != 20*time.Millisecond || spec.Target != "" {
		t.Fatalf("latency: %+v %v", spec, err)
	}
	bad := []struct {
		name string
		req  wire.FaultRequest
		want string
	}{
		{"latency without mechanism is a toxic", netReq("latency", "", "", 0, `{"delay":"100ms"}`), "toxic"},
		{"downstream is ingress", netReq("packet_loss", "x", "downstream", 0, `{"loss":10}`), "egress only"},
		{"latency scope", netReq("latency", "", "", 0.5, `{"delay":"100ms","mechanism":"netem"}`), "scope"},
		{"other type", netReq("rst", "x", "", 0, `{"mechanism":"netem"}`), "packet_loss and latency only"},
		{"bad params", netReq("packet_loss", "x", "", 0, `{"loss":0}`), "loss"},
		{"no envelope", wire.FaultRequest{Category: "network", FaultType: "packet_loss", Params: json.RawMessage(`{"loss":1}`)}, "envelope"},
		{"not network", wire.FaultRequest{Category: "inline", FaultType: "latency", Params: json.RawMessage(`{"delay":"1ms"}`)}, "network"},
		{"ramp up", withRamps(netReq("packet_loss", "", "", 0, `{"loss":10}`), 1000, 0), "netem: ramps are not supported"},
		{"ramp down", withRamps(netReq("latency", "", "", 0, `{"delay":"1ms","mechanism":"netem"}`), 0, 500), "netem: ramps are not supported"},
		{"malformed port", netReq("packet_loss", "catalog:abc", "", 0, `{"loss":10}`), "port"},
		{"empty port", netReq("packet_loss", "catalog:", "", 0, `{"loss":10}`), "port"},
		{"port out of range", netReq("packet_loss", "catalog:70000", "", 0, `{"loss":10}`), "port"},
		{"bracketed IPv6 literal", netReq("packet_loss", "[fe80::1]:80", "", 0, `{"loss":10}`), "IPv6"},
		{"bare IPv6 literal", netReq("packet_loss", "fe80::1", "", 0, `{"loss":10}`), "IPv6"},
	}
	for _, c := range bad {
		if _, err := NetemSpecFromRequest(c.req); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.want)
		}
	}
}

func withRamps(r wire.FaultRequest, up, down int64) wire.FaultRequest {
	r.RampUpMs, r.RampDownMs = up, down
	return r
}

// Minor 5: scope × loss is rounded to 6 significant digits before it is
// formatted (0.1 × 3 is 0.30000000000000004 in float64).
func TestNetemArgs_RoundsTheScaledLoss(t *testing.T) {
	spec, err := NetemSpecFromRequest(netReq("packet_loss", "", "", 0.1, `{"loss":3,"correlation":25}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := netemArgs(spec); !reflect.DeepEqual(got, []string{"loss", "0.3%", "25%"}) {
		t.Fatalf("netemArgs = %v, want [loss 0.3%% 25%%]", got)
	}
	if got := netemArgs(NetemSpec{FaultType: "packet_loss", Loss: 1.0 / 3}); !reflect.DeepEqual(got, []string{"loss", "0.333333%"}) {
		t.Fatalf("netemArgs = %v, want 6 significant digits", got)
	}
}

func TestNetemArgs(t *testing.T) {
	cases := []struct {
		spec NetemSpec
		want []string
	}{
		{NetemSpec{FaultType: "packet_loss", Loss: 10, Correlation: 25}, []string{"loss", "10%", "25%"}},
		{NetemSpec{FaultType: "packet_loss", Loss: 12.5}, []string{"loss", "12.5%"}},
		{NetemSpec{FaultType: "latency", Delay: 100 * time.Millisecond, Jitter: 20 * time.Millisecond}, []string{"delay", "100ms", "20ms"}},
		{NetemSpec{FaultType: "latency", Delay: 1500 * time.Microsecond}, []string{"delay", "1.5ms"}},
	}
	for _, c := range cases {
		if got := netemArgs(c.spec); !reflect.DeepEqual(got, c.want) {
			t.Errorf("netemArgs(%+v) = %v, want %v", c.spec, got, c.want)
		}
	}
}

func TestNetemPlan_WholePodAndTargeted(t *testing.T) {
	spec := NetemSpec{FaultType: "packet_loss", Loss: 10, Correlation: 25}
	whole := netemPlan("eth0", nil, 0, spec)
	if len(whole) != 1 || strings.Join(whole[0], " ") != "tc qdisc add dev eth0 root handle 1: netem loss 10% 25%" {
		t.Fatalf("whole-pod plan = %v", whole)
	}
	ips := []net.IP{net.ParseIP("10.43.0.5"), net.ParseIP("10.43.0.6")}
	got := netemPlan("eth0", ips, 3550, spec)
	want := []string{
		"tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
		"tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.6/32 match ip dport 3550 0xffff flowid 1:4",
	}
	for i := range want {
		if i >= len(got) || strings.Join(got[i], " ") != want[i] {
			t.Fatalf("plan[%d] = %v, want %q", i, got, want[i])
		}
	}
	noPort := netemPlan("eth0", ips[:1], 0, spec)
	if l := strings.Join(noPort[2], " "); strings.Contains(l, "dport") {
		t.Fatalf("a target without a port must not match dport: %s", l)
	}
}

const ipLinkJSON = `[{"ifindex":1,"ifname":"lo","flags":["LOOPBACK","UP","LOWER_UP"],"link_type":"loopback"},` +
	`{"ifindex":2,"ifname":"eth1","flags":["BROADCAST","MULTICAST"],"link_type":"ether"},` +
	`{"ifindex":3,"ifname":"eth0","flags":["BROADCAST","MULTICAST","UP","LOWER_UP"],"link_type":"ether"}]`

func TestPickInterface(t *testing.T) {
	if got := pickInterface([]byte(ipLinkJSON)); got != "eth0" {
		t.Fatalf("pickInterface = %q, want eth0 (up, ether, not loopback)", got)
	}
	if got := pickInterface([]byte(`garbage`)); got != "" {
		t.Fatalf("unparseable listing must yield \"\", got %q", got)
	}
	if got := pickInterface([]byte(`[{"ifname":"lo","flags":["LOOPBACK","UP"],"link_type":"loopback"}]`)); got != "" {
		t.Fatalf("loopback only must yield \"\", got %q", got)
	}
}

func TestNetemSpecFromRequest_ScopeRange(t *testing.T) {
	cases := []struct {
		name     string
		scope    float64
		wantLoss float64
		wantErr  string
	}{
		{"zero means one", 0, 10, ""},
		{"half is applied", 0.5, 5, ""},
		{"negative", -0.5, 0, "network scope -0.5 outside (0,1]"},
		{"above one", 1.5, 0, "network scope 1.5 outside (0,1]"},
	}
	for _, c := range cases {
		spec, err := NetemSpecFromRequest(netReq("packet_loss", "x", "", c.scope, `{"loss":10}`))
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("%s: err = %v, want one mentioning %q", c.name, err, c.wantErr)
			}
			continue
		}
		if err != nil || spec.Loss != c.wantLoss {
			t.Errorf("%s: spec = %+v, err = %v, want loss %v", c.name, spec, err, c.wantLoss)
		}
	}
}
