package proxy

import (
	"sync/atomic"

	"bufio"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/interceptor"
)

func TestParseSockaddrIn(t *testing.T) {
	b := make([]byte, 16)
	binary.NativeEndian.PutUint16(b[0:2], afInet)
	binary.BigEndian.PutUint16(b[2:4], 3550)
	copy(b[4:8], []byte{10, 42, 0, 7})
	got, err := parseSockaddrIn(b)
	if err != nil || got != "10.42.0.7:3550" {
		t.Fatalf("got %q, %v", got, err)
	}
	binary.NativeEndian.PutUint16(b[0:2], 10) // AF_INET6
	if _, err := parseSockaddrIn(b); err == nil {
		t.Fatal("non-AF_INET must error")
	}
	if _, err := parseSockaddrIn(b[:4]); err == nil {
		t.Fatal("short buffer must error")
	}
}

func fixedDst(addr string) OriginalDstFunc {
	return func(net.Conn) (string, error) { return addr, nil }
}

func transparentBase(table *PolicyTable) http.RoundTripper {
	return &ToxicTransport{Table: table, Base: NewTransparentTransport()}
}

func TestTransparentEgress_DialsOriginalDstKeepsHostAndKey(t *testing.T) {
	ti := newTestInstrument(t)
	up := newUpstream(t, `{"live":true}`)
	table := NewPolicyTable()
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Base: transparentBase(table), Policies: table,
		TransparentEgress: &TransparentEgress{Listen: "127.0.0.1:0"}, OriginalDst: fixedDst(hostPort(up.ts))})
	ti.rules(t, cacheBoxRule("passthrough"))

	// The app dialed "productcatalogservice:3550" (redirected here); its
	// Host header says so, and the sidecar must key on that name.
	req, _ := http.NewRequest("GET", "http://"+p.TransparentEgressAddr()+"/api/items?b=2&a=1", nil)
	req.Host = "productcatalogservice:3550"
	req.Header.Set("Accept", "application/json")
	resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	up.mu.Lock()
	gotHost, gotURI := up.hosts[0], up.uris[0]
	up.mu.Unlock()
	if gotHost != "productcatalogservice:3550" || gotURI != "/api/items?b=2&a=1" {
		t.Fatalf("upstream saw host=%q uri=%q; transparent mode must preserve the app's Host", gotHost, gotURI)
	}

	// INV-2 across capture modes: the same key the in-process host derives
	// for the client-side request the app made.
	k1 := resp.Header.Get(interceptor.HeaderCacheKey)
	fake := roundTripperFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: http.NoBody}, nil
	})
	direct, _ := http.NewRequest("GET", "http://productcatalogservice:3550/api/items?b=2&a=1", nil)
	direct.Header.Set("Accept", "application/json")
	dresp, err := instrument.EgressTransport(fake, ti.resolve).RoundTrip(direct)
	if err != nil {
		t.Fatal(err)
	}
	dresp.Body.Close()
	if k2 := dresp.Header.Get(interceptor.HeaderCacheKey); k1 == "" || k1 != k2 {
		t.Fatalf("transparent key %q != in-process key %q", k1, k2)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransparentEgress_LoopGuardAndMissingDst(t *testing.T) {
	ti := newTestInstrument(t)
	up := newUpstream(t, `x`)

	// Loop: original destination is the listener itself. The accept
	// goroutine reads what the test writes after Start, hence the atomic.
	var self atomic.Value
	self.Store("")
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Base: transparentBase(nil),
		TransparentEgress: &TransparentEgress{Listen: "127.0.0.1:0"},
		OriginalDst:       func(net.Conn) (string, error) { return self.Load().(string), nil }})
	self.Store(p.TransparentEgressAddr())
	resp, body := doGet(t, &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}, "http://"+p.TransparentEgressAddr()+"/x", nil)
	if resp.StatusCode != 502 || !strings.Contains(body, "itself") || up.calls() != 0 {
		t.Fatalf("loop guard: status=%d body=%q calls=%d", resp.StatusCode, body, up.calls())
	}

	// No original destination available.
	p2 := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Base: transparentBase(nil),
		TransparentEgress: &TransparentEgress{Listen: "127.0.0.1:0"},
		OriginalDst:       func(net.Conn) (string, error) { return "", ErrOriginalDstUnsupported }})
	resp, body = doGet(t, &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}, "http://"+p2.TransparentEgressAddr()+"/x", nil)
	if resp.StatusCode != 502 || !strings.Contains(body, "original destination") {
		t.Fatalf("missing dst: status=%d body=%q", resp.StatusCode, body)
	}
}

func TestTransparentIngress_RoutesByOriginalPortAndRejectsOthers(t *testing.T) {
	ti := newTestInstrument(t)
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("app:" + r.Host)) }))
	t.Cleanup(app.Close)
	_, appPort, _ := net.SplitHostPort(hostPort(app))
	port, _ := strconv.Atoi(appPort)

	var dst atomic.Value // read by the accept goroutine, rewritten by the test
	dst.Store("10.42.0.7:" + appPort)
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve,
		TransparentIngress: &TransparentIngress{Listen: "127.0.0.1:0", AppHost: "127.0.0.1", AllowedPorts: []int{port}},
		OriginalDst:        func(net.Conn) (string, error) { return dst.Load().(string), nil }})
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	req, _ := http.NewRequest("GET", "http://"+p.TransparentIngressAddr()+"/cart", nil)
	req.Host = "frontend:8080"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 64)
	n, _ := resp.Body.Read(b)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(b[:n]) != "app:frontend:8080" {
		t.Fatalf("status=%d body=%q", resp.StatusCode, b[:n])
	}

	dst.Store("10.42.0.7:9999")
	resp, body := doGet(t, client, "http://"+p.TransparentIngressAddr()+"/cart", nil)
	if resp.StatusCode != 502 || !strings.Contains(body, "not an inbound app port") {
		t.Fatalf("disallowed port: status=%d body=%q", resp.StatusCode, body)
	}
}

// echoServer is a raw TCP upstream: it echoes lines.
func echoServer(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					c.Write([]byte(line))
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func rawExchange(t *testing.T, addr, msg string) (string, time.Duration) {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	if _, err := c.Write([]byte(msg + "\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return strings.TrimSpace(line), time.Since(start)
}

func TestRawRelay_EchoesAndAppliesArmedToxicByResolvedName(t *testing.T) {
	ti := newTestInstrument(t)
	echo := echoServer(t)
	_, echoPort, _ := net.SplitHostPort(echo)
	port, _ := strconv.Atoi(echoPort)
	table := NewPolicyTable()
	table.lookup = func(host string) []net.IP {
		if host == "redis-cart" {
			return []net.IP{net.ParseIP("127.0.0.1")}
		}
		return nil
	}
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Base: transparentBase(table), Policies: table,
		TransparentEgress: &TransparentEgress{Listen: "127.0.0.1:0", RawTCPPorts: []int{port}}, OriginalDst: fixedDst(echo)})

	if got, _ := rawExchange(t, p.TransparentEgressAddr(), "ping"); got != "ping" {
		t.Fatalf("relay echo = %q", got)
	}

	// A latency toxic armed for the dependency by NAME shapes the relayed
	// connection (matched by resolving the name against the original
	// destination address).
	armFor(t, table, Policy{Name: "lat", Target: "redis-cart:" + echoPort, Direction: network.Downstream, Toxic: &network.Latency{Delay: 150 * time.Millisecond}})
	got, el := rawExchange(t, p.TransparentEgressAddr(), "pong")
	if got != "pong" || el < 150*time.Millisecond {
		t.Fatalf("shaped relay: echo=%q elapsed=%v", got, el)
	}
}

func TestRawRelay_BlackholeNeverDialsAndRSTResets(t *testing.T) {
	ti := newTestInstrument(t)
	echo := echoServer(t)
	_, echoPort, _ := net.SplitHostPort(echo)
	port, _ := strconv.Atoi(echoPort)
	table := NewPolicyTable()
	p := startProxy(t, Config{Service: "svc", Resolve: ti.resolve, Base: transparentBase(table), Policies: table,
		TransparentEgress: &TransparentEgress{Listen: "127.0.0.1:0", RawTCPPorts: []int{port}}, OriginalDst: fixedDst(echo)})

	armFor(t, table, Policy{Name: "bh", Target: echo, Toxic: &network.Blackhole{}})
	c, err := net.Dial("tcp", p.TransparentEgressAddr())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(300 * time.Millisecond))
	c.Write([]byte("hello\n"))
	if _, err := bufio.NewReader(c).ReadString('\n'); err == nil {
		t.Fatal("blackholed connection must never answer")
	}
	c.Close()

	table.disarm(table.armed["bh"])
	armFor(t, table, Policy{Name: "rst", Target: echo, Direction: network.Downstream, Toxic: &network.RST{AfterBytes: 1}})
	c, err = net.Dial("tcp", p.TransparentEgressAddr())
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	c.Write([]byte("hello\n"))
	buf := make([]byte, 64)
	_, rerr := c.Read(buf) // one byte may arrive; then the reset
	if rerr == nil {
		_, rerr = c.Read(buf)
	}
	c.Close()
	if rerr == nil {
		t.Fatal("reset connection must error on read")
	}
}
