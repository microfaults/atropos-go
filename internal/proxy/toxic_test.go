package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
)

func armFor(t *testing.T, table *PolicyTable, p Policy) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	table.arm(ctx, p)
}

func TestPolicyTable_MatchTargetsAndScope(t *testing.T) {
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "a", Target: "dep:80", Toxic: &network.Latency{}})
	armFor(t, table, Policy{Name: "b", Target: "other", Toxic: &network.Latency{}})

	if _, ok := table.match("dep:80"); !ok {
		t.Fatal("exact host:port must match")
	}
	if _, ok := table.match("dep:81"); ok {
		t.Fatal("a target with a port must not match another port")
	}
	if a, ok := table.match("other:9999"); !ok || a.Name != "b" {
		t.Fatal("a port-less target must match the hostname on any port")
	}
	if _, ok := table.match("nobody:1"); ok {
		t.Fatal("unrelated host must not match")
	}

	// Scope: a roll at or above the scope is not affected but counted as matched.
	table.roll = func() float64 { return 0.9 }
	armFor(t, table, Policy{Name: "c", Target: "half:1", Scope: 0.5, Toxic: &network.Latency{}})
	if _, ok := table.match("half:1"); ok {
		t.Fatal("roll 0.9 >= scope 0.5 must not be affected")
	}
	table.roll = func() float64 { return 0.1 }
	a, ok := table.match("half:1")
	if !ok || a.matched.Load() != 2 || a.affected.Load() != 1 {
		t.Fatalf("roll 0.1 < scope 0.5 must be affected; matched=%d affected=%d", a.matched.Load(), a.affected.Load())
	}
}

func TestPolicyFault_ArmsForDurationAndDisarmsOnStop(t *testing.T) {
	table := NewPolicyTable()
	f := &PolicyFault{FaultConfig: fault.FaultConfig{Duration: 10 * time.Second}, Table: table,
		Policy: Policy{Name: "r1", Target: "dep:80", Toxic: &network.Latency{Delay: time.Millisecond}}}
	h, err := f.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !table.Armed("r1") {
		t.Fatal("Start must arm immediately")
	}
	table.match("dep:80")
	h.Stop()
	select {
	case res := <-h.Done():
		if d, ok := res.Detail.(PolicyDetail); !ok || d.Matched != 1 || d.Affected != 1 {
			t.Fatalf("detail = %#v", res.Detail)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stopped fault did not deliver a result")
	}
	if table.Armed("r1") {
		t.Fatal("Stop must disarm")
	}
}

func TestPolicyFault_RearmSupersedesAndOldExpiryDoesNotDisarm(t *testing.T) {
	table := NewPolicyTable()
	mk := func() *PolicyFault {
		return &PolicyFault{FaultConfig: fault.FaultConfig{Duration: 10 * time.Second}, Table: table,
			Policy: Policy{Name: "r1", Target: "dep:80", Toxic: &network.Latency{}}}
	}
	h1, _ := mk().Start(context.Background())
	h2, _ := mk().Start(context.Background())
	h1.Stop()
	<-h1.Done()
	if !table.Armed("r1") {
		t.Fatal("the predecessor's expiry must not disarm the re-armed policy")
	}
	h2.Stop()
	<-h2.Done()
}

type countingRT struct {
	calls atomic.Int32
	next  http.RoundTripper
}

func (c *countingRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.calls.Add(1)
	return c.next.RoundTrip(r)
}

func TestToxicTransport_LatencyDownstreamDelaysResponseIntact(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello world")) }))
	t.Cleanup(up.Close)
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "lat", Target: hostPort(up), Direction: network.Downstream, Toxic: &network.Latency{Delay: 150 * time.Millisecond}})
	client := &http.Client{Transport: &ToxicTransport{Table: table}}

	start := time.Now()
	resp, err := client.Get(up.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("downstream latency not applied: %v", el)
	}
	if string(body) != "hello world" {
		t.Fatalf("body = %q", body)
	}
}

func TestToxicTransport_UpstreamDirectionShapesRequestBody(t *testing.T) {
	var got []byte
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { got, _ = io.ReadAll(r.Body) }))
	t.Cleanup(up.Close)
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "lat", Target: hostPort(up), Direction: network.Upstream, Toxic: &network.Latency{Delay: 120 * time.Millisecond}})
	client := &http.Client{Transport: &ToxicTransport{Table: table}}

	start := time.Now()
	resp, err := client.Post(up.URL+"/x", "text/plain", bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if el := time.Since(start); el < 120*time.Millisecond {
		t.Fatalf("upstream latency not applied: %v", el)
	}
	if string(got) != "payload" {
		t.Fatalf("upstream got %q", got)
	}
}

func TestToxicTransport_UnmatchedHostPassesThrough(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	t.Cleanup(up.Close)
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "lat", Target: "somewhere-else:1", Toxic: &network.Latency{Delay: time.Second}})
	client := &http.Client{Transport: &ToxicTransport{Table: table}}
	start := time.Now()
	resp, err := client.Get(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("unmatched host must not be shaped")
	}
}

func TestToxicTransport_BlackholeReturnsHijackErrorWithoutCallingBase(t *testing.T) {
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "bh", Target: "dep:80", Toxic: &network.Blackhole{}})
	base := &countingRT{next: http.DefaultTransport}
	req, _ := http.NewRequest("GET", "http://dep:80/x", nil)
	_, err := (&ToxicTransport{Base: base, Table: table}).RoundTrip(req)
	var hj *HijackError
	if !errors.As(err, &hj) || hj.Mode != HijackBlackhole || hj.Hold == nil {
		t.Fatalf("err = %v, want blackhole HijackError", err)
	}
	if base.calls.Load() != 0 {
		t.Fatal("blackhole must not reach the upstream")
	}
}

func TestToxicTransport_RSTReturnsHijackErrorWithResponse(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("hello")) }))
	t.Cleanup(up.Close)
	table := NewPolicyTable()
	armFor(t, table, Policy{Name: "rst", Target: hostPort(up), Toxic: &network.RST{AfterBytes: 3}})
	base := &countingRT{next: http.DefaultTransport}
	req, _ := http.NewRequest("GET", up.URL+"/x", nil)
	_, err := (&ToxicTransport{Base: base, Table: table}).RoundTrip(req)
	var hj *HijackError
	if !errors.As(err, &hj) || hj.Mode != HijackRST || hj.Resp == nil || hj.AfterBytes != 3 {
		t.Fatalf("err = %v, want rst HijackError carrying the response", err)
	}
	hj.Resp.Body.Close()
	if base.calls.Load() != 1 {
		t.Fatal("rst forwards the request once")
	}
}

// ---- through the real egress listener, armed via the control surface ----

type sidecarInstrument struct {
	*testInstrument
	table *PolicyTable
}

func newSidecarInstrument(t *testing.T) *sidecarInstrument {
	t.Helper()
	table := NewPolicyTable()
	core := instrument.NewCore(instrument.CoreConfig{
		Service: "svc", Instance: "pod-1", Logger: slog.New(slog.DiscardHandler),
		EagerArm:      true,
		DecodeOptions: []instrument.DecodeOption{instrument.WithInlineToxicHost(InlineToxicFactory(table))},
	})
	t.Cleanup(core.Stop)
	control := httptest.NewServer(instrument.ControlSurface(instrument.ControlConfig{Core: core}))
	t.Cleanup(control.Close)
	return &sidecarInstrument{testInstrument: &testInstrument{core: core, control: control}, table: table}
}

func inlineNetworkRule(name, faultType, target, direction, params string) string {
	return fmt.Sprintf(`[{"name":%q,"injection_point":"egress","mode":"background",
	  "fault":{"category":"network","fault_type":%q,"duration_ms":10000,
	    "network":{"host":"inline","target":%q,"direction":%q},"params":%s}}]`, name, faultType, target, direction, params)
}

func (si *sidecarInstrument) startEgress(t *testing.T, up *httptest.Server) *Proxy {
	t.Helper()
	return startProxy(t, Config{Service: "svc", Resolve: si.resolve,
		Base:   &ToxicTransport{Table: si.table},
		Egress: []EgressListener{{Listen: "127.0.0.1:0", Upstream: hostPort(up)}}})
}

func freshClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: &http.Transport{DisableKeepAlives: true}}
}

func TestEagerArm_NetworkRuleAppliesWithoutTraffic(t *testing.T) {
	si := newSidecarInstrument(t)
	si.rules(t, inlineNetworkRule("lat", "latency", "dep:80", "downstream", `{"delay":"50ms"}`))
	if !si.table.Armed("lat") {
		t.Fatal("a network rule must be armed when it applies, before any traffic")
	}
	si.rules(t, `[]`)
	deadline := time.Now().Add(2 * time.Second)
	for si.table.Armed("lat") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if si.table.Armed("lat") {
		t.Fatal("clearing the rule set must disarm the policy (rule removal is the stop signal)")
	}
}

func TestEgress_InlineLatencyToxicThroughListener(t *testing.T) {
	si := newSidecarInstrument(t)
	up := newUpstream(t, `ok`)
	p := si.startEgress(t, up.ts)
	si.rules(t, inlineNetworkRule("lat", "latency", hostPort(up.ts), "downstream", `{"delay":"150ms"}`))

	start := time.Now()
	resp, body := doGet(t, freshClient(5*time.Second), "http://"+p.EgressAddrs()[0]+"/x", nil)
	if resp.StatusCode != 200 || body != "ok" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	if el := time.Since(start); el < 150*time.Millisecond {
		t.Fatalf("network latency toxic not applied through the listener: %v", el)
	}
}

func TestEgress_RSTResetsClientConnection(t *testing.T) {
	si := newSidecarInstrument(t)
	up := newUpstream(t, strings.Repeat("x", 4096))
	p := si.startEgress(t, up.ts)
	si.rules(t, inlineNetworkRule("rst", "rst", hostPort(up.ts), "downstream", `{"after_bytes":16}`))

	_, err := freshClient(5 * time.Second).Get("http://" + p.EgressAddrs()[0] + "/x")
	if err == nil {
		t.Fatal("the app must observe a connection failure, not a proxy-invented HTTP error")
	}
	if up.calls() != 1 {
		t.Fatalf("upstream calls = %d, want 1 (rst forwards then resets)", up.calls())
	}
}

func TestEgress_BlackholeHoldsUntilClientTimeout(t *testing.T) {
	si := newSidecarInstrument(t)
	up := newUpstream(t, `never`)
	p := si.startEgress(t, up.ts)
	si.rules(t, inlineNetworkRule("bh", "blackhole", hostPort(up.ts), "downstream", `{}`))

	start := time.Now()
	_, err := freshClient(300 * time.Millisecond).Get("http://" + p.EgressAddrs()[0] + "/x")
	var nerr net.Error
	if err == nil || !(errors.As(err, &nerr) && nerr.Timeout()) {
		t.Fatalf("err = %v, want the client's own timeout", err)
	}
	if el := time.Since(start); el < 250*time.Millisecond {
		t.Fatalf("returned before the client timeout: %v", el)
	}
	if up.calls() != 0 {
		t.Fatalf("upstream calls = %d, want 0 (blackholed requests never reach the upstream)", up.calls())
	}
}
