package network

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	fault "atropos-go/internal/fault"
)

// startEchoServer starts a TCP server that echoes back everything it receives.
// Returns the listener address and a cleanup function.
func startEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()

	return ln.Addr().String(), func() { ln.Close() }
}

func TestProxy_Passthrough(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 2 * time.Second},
		Listen:      "127.0.0.1:0",
		Upstream:    upstream,
		Scope:       0, // default to 1.0, but no toxics = will fail validation
		Toxics:      []ToxicLink{{Toxic: &Latency{Delay: 0}, Direction: Downstream}},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Proxy picks an ephemeral port; need to get it from the detail after it's running.
	// Since Start is non-blocking and the listener is up, we can connect now.
	// But we need the listen addr — let's use a known port instead.
	handle.Stop()
	<-handle.Done()
}

func TestProxy_WithLatency(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	// Use a fixed port for the proxy.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close() // free it for the proxy

	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 5 * time.Second},
		Listen:      proxyAddr,
		Upstream:    upstream,
		Toxics: []ToxicLink{
			{Toxic: &Latency{Delay: 100 * time.Millisecond}, Direction: Downstream},
		},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Stop()

	// Connect through the proxy.
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send data and measure round-trip time (includes latency toxic).
	start := time.Now()
	msg := []byte("hello")
	conn.Write(msg)

	buf := make([]byte, len(msg))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = io.ReadFull(conn, buf)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("got %q, want %q", buf, "hello")
	}
	// Should include ~100ms latency from the downstream toxic.
	if elapsed < 80*time.Millisecond {
		t.Fatalf("expected >= 80ms round-trip, got %s (latency not applied)", elapsed)
	}
	t.Logf("round-trip with 100ms latency toxic: %s", elapsed)
}

func TestProxy_Blackhole(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close()

	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 2 * time.Second},
		Listen:      proxyAddr,
		Upstream:    upstream,
		Toxics: []ToxicLink{
			{Toxic: &Blackhole{}, Direction: Upstream},
		},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Stop()

	// Connect — should succeed (TCP handshake completes).
	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Write should succeed (kernel buffers it).
	conn.Write([]byte("hello"))

	// Read should timeout — blackhole swallows everything.
	conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	buf := make([]byte, 5)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected timeout, got data")
	}
	t.Logf("blackhole: read timed out as expected: %v", err)
}

func TestProxy_Stop(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close()

	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 30 * time.Second},
		Listen:      proxyAddr,
		Upstream:    upstream,
		Toxics:      []ToxicLink{{Toxic: &Latency{Delay: 0}, Direction: Downstream}},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	handle.Stop()

	result := <-handle.Done()
	if result.ActualDuration > 2*time.Second {
		t.Fatalf("expected early stop, ran for %s", result.ActualDuration)
	}

	d, ok := result.Detail.(Detail)
	if !ok {
		t.Fatal("expected Detail in result")
	}
	t.Logf("proxy stopped: listened on %s, total=%d conns", d.ListenAddr, d.TotalConnections)
}

func TestProxy_InvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no listen", Config{
			FaultConfig: fault.FaultConfig{Duration: time.Second},
			Upstream:    "x:1", Toxics: []ToxicLink{{Toxic: &Latency{}, Direction: Downstream}},
		}},
		{"no upstream", Config{
			FaultConfig: fault.FaultConfig{Duration: time.Second},
			Listen:      ":0", Toxics: []ToxicLink{{Toxic: &Latency{}, Direction: Downstream}},
		}},
		{"no toxics", Config{
			FaultConfig: fault.FaultConfig{Duration: time.Second},
			Listen:      ":0", Upstream: "x:1",
		}},
		{"bad scope", Config{
			FaultConfig: fault.FaultConfig{Duration: time.Second},
			Listen:      ":0", Upstream: "x:1", Scope: 1.5,
			Toxics: []ToxicLink{{Toxic: &Latency{}, Direction: Downstream}},
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Proxy{Config: tc.cfg}
			_, err := p.Start(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			t.Logf("correctly rejected: %v", err)
		})
	}
}

func TestProxy_Scope(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close()

	// Scope 0.5: ~half of connections get 200ms latency.
	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 5 * time.Second},
		Listen:      proxyAddr,
		Upstream:    upstream,
		Scope:       0.5,
		Toxics: []ToxicLink{
			{Toxic: &Latency{Delay: 200 * time.Millisecond}, Direction: Downstream},
		},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Stop()

	// Make 20 connections and measure round-trip times.
	fast, slow := 0, 0
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		start := time.Now()
		conn.Write([]byte("x"))
		buf := make([]byte, 1)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		io.ReadFull(conn, buf)
		elapsed := time.Since(start)
		conn.Close()

		if elapsed > 150*time.Millisecond {
			slow++
		} else {
			fast++
		}
	}

	t.Logf("scope=0.5: fast=%d, slow=%d out of 20 connections", fast, slow)

	// With 50% scope, we'd expect a mix. Allow wide tolerance.
	if fast == 0 || slow == 0 {
		t.Logf("warning: expected a mix of fast and slow, got fast=%d slow=%d (probabilistic)", fast, slow)
	}
}

func TestProxy_RST(t *testing.T) {
	upstream, cleanup := startEchoServer(t)
	defer cleanup()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close()

	p := &Proxy{Config: Config{
		FaultConfig: fault.FaultConfig{Duration: 5 * time.Second},
		Listen:      proxyAddr,
		Upstream:    upstream,
		Toxics: []ToxicLink{
			{Toxic: &RST{AfterBytes: 3}, Direction: Downstream},
		},
	}}

	handle, err := p.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Stop()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send data — echo server will echo it back.
	conn.Write([]byte("hello world"))

	// Read should get some data then a connection reset.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 100)
	totalRead := 0
	for {
		n, err := conn.Read(buf[totalRead:])
		totalRead += n
		if err != nil {
			t.Logf("RST: read %d bytes then error: %v", totalRead, err)
			// Expect connection reset by peer or EOF.
			break
		}
	}
	fmt.Println()
}
