package agent

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// recRunner records every command line and answers from substring tables.
type recRunner struct {
	mu    sync.Mutex
	calls []string
	out   map[string][]byte // substring of the line → stdout
	fail  map[string]error  // substring of the line → error (output "RTNETLINK answers: Invalid argument")
	seen  func(line string) // optional probe, called before answering
}

func (r *recRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	line := strings.Join(append([]string{name}, args...), " ")
	r.mu.Lock()
	r.calls = append(r.calls, line)
	r.mu.Unlock()
	if r.seen != nil {
		r.seen(line)
	}
	for k, err := range r.fail {
		if strings.Contains(line, k) {
			return []byte("RTNETLINK answers: Invalid argument"), err
		}
	}
	for k, out := range r.out {
		if strings.Contains(line, k) {
			return out, nil
		}
	}
	return nil, nil
}

func (r *recRunner) lines() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *recRunner) has(sub string) bool {
	for _, l := range r.lines() {
		if strings.Contains(l, sub) {
			return true
		}
	}
	return false
}

// newTestNetem builds a Netem over a fake /proc (pid 4242 in net:[4026532412]),
// a recording runner and a resolver answering 10.43.0.5. The target's
// cgroup.procs names pid 4242.
func newTestNetem(t *testing.T) (*Netem, *recRunner, string, Target) {
	t.Helper()
	proc := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proc, "4242", "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	netns := filepath.Join(proc, "4242", "ns", "net")
	if err := os.Symlink("net:[4026532412]", netns); err != nil {
		t.Fatal(err)
	}
	r := &recRunner{out: map[string][]byte{"ip -json link show": []byte(ipLinkJSON)}, fail: map[string]error{}}
	n := &Netem{Runner: r, Journal: NetemJournal{Dir: filepath.Join(t.TempDir(), "netem")}, ProcRoot: proc,
		LookupIP: func(_ context.Context, host string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.43.0.5")}}, nil
		}}
	tg := Target{PodUID: "pod-a", Container: "server", ContainerID: "c1", CgroupDir: cgroupDir(t, map[string]string{"cgroup.procs": "4242\n"})}
	return n, r, netns, tg
}

var lossSpec = NetemSpec{FaultType: "packet_loss", Target: "productcatalogservice:3550", Loss: 10, Correlation: 25}

func TestNetem_ApplyJournalsBeforeTCAndRestoreRemovesEntry(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	journaledBeforeTC := false
	r.seen = func(line string) {
		if strings.Contains(line, " tc qdisc add ") && !journaledBeforeTC {
			es, _ := n.Journal.Load()
			journaledBeforeTC = len(es) == 1
		}
	}
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	if !journaledBeforeTC {
		t.Fatal("the journal entry must exist before the first tc command runs")
	}
	ns := "--net=" + netns
	want := []string{
		"nsenter " + ns + " ip -json link show",
		"nsenter " + ns + " tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1",
		"nsenter " + ns + " tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 10% 25%",
		"nsenter " + ns + " tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.43.0.5/32 match ip dport 3550 0xffff flowid 1:4",
	}
	if got := r.lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commands:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if e.Netns != netns || e.NetnsID != "net:[4026532412]" || e.Dev != "eth0" || e.PodUID != "pod-a" || len(e.Applied) != 3 {
		t.Fatalf("entry = %+v", e)
	}
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}
	if last := r.lines()[len(r.lines())-1]; last != "nsenter "+ns+" tc qdisc del dev eth0 root" {
		t.Fatalf("restore ran %q", last)
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after restore")
	}
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
		t.Fatalf("the namespace must be free again after restore: %v", err)
	}
}

func TestNetem_WholePodLatencyUsesTheRootQdisc(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "latency", Delay: 100 * time.Millisecond, Jitter: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if got := r.lines(); len(got) != 2 || got[1] != "nsenter --net="+netns+" tc qdisc add dev eth0 root handle 1: netem delay 100ms 20ms" {
		t.Fatalf("commands = %q", got)
	}
}

func TestNetem_RefusesASecondFaultInTheSameNamespace(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "f1", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	before := len(r.lines())
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); !errors.Is(err, ErrNetemBusy) {
		t.Fatalf("second apply: err = %v, want ErrNetemBusy", err)
	}
	if len(r.lines()) != before {
		t.Fatal("a refused apply must not run any command")
	}
}

func TestNetem_ApplyRollsBackWhenACommandFails(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["tc filter add"] = errors.New("exit status 2")
	_, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err == nil || !strings.Contains(err.Error(), "exit status 2") {
		t.Fatalf("err = %v, want the tc failure", err)
	}
	if !r.has("tc qdisc del dev eth0 root") {
		t.Fatal("the root qdisc added before the failure must be deleted again")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after a rolled-back apply")
	}
	delete(r.fail, "tc filter add")
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
		t.Fatalf("the namespace must be released after a failed apply: %v", err)
	}
}

func TestNetem_RestoreSkipsAChangedNamespace(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(netns)
	if err := os.Symlink("net:[4026539999]", netns); err != nil { // the pid now belongs to another namespace
		t.Fatal(err)
	}
	before := len(r.lines())
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatalf("restore of a vanished namespace must succeed: %v", err)
	}
	if len(r.lines()) != before {
		t.Fatal("restore must not run tc in a namespace that is not the journaled one")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("the stale entry must be dropped")
	}
}

func TestNetem_RestoreAllAtBoot(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "crashed", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	// "Agent restarts": a fresh Netem over the same journal and runner.
	boot := &Netem{Runner: r, Journal: n.Journal, ProcRoot: n.ProcRoot}
	if got := boot.RestoreAll(context.Background(), slog.New(slog.DiscardHandler)); got != 1 {
		t.Fatalf("RestoreAll = %d, want 1", got)
	}
	if !r.has("tc qdisc del dev eth0 root") {
		t.Fatal("boot restore must delete the journaled qdisc")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after boot restore")
	}
}

func TestNetem_FallsBackToEth0WhenLinkListingFails(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["ip -json link show"] = errors.New("exit status 1")
	e, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "packet_loss", Loss: 1})
	if err != nil || e.Dev != "eth0" {
		t.Fatalf("entry=%+v err=%v, want eth0", e, err)
	}
}

func TestNetem_ResolvesTargetToEveryIPv4(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	n.LookupIP = func(_ context.Context, host string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.43.0.5")}, {IP: net.ParseIP("fe80::1")}, {IP: net.ParseIP("10.43.0.6")}}, nil
	}
	e, err := n.Apply(context.Background(), "f1", tg, NetemSpec{FaultType: "packet_loss", Target: "catalog", Loss: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !r.has("match ip dst 10.43.0.5/32 flowid") || !r.has("match ip dst 10.43.0.6/32 flowid") || r.has("fe80") {
		t.Fatalf("filters = %q, want one per IPv4 address and no IPv6", r.lines())
	}
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}

	// IPv6-only target: rejected before any tc command, namespace released.
	n.LookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("fe80::1")}}, nil
	}
	before := len(r.lines())
	if _, err := n.Apply(context.Background(), "f2", tg, NetemSpec{FaultType: "packet_loss", Target: "v6only", Loss: 1}); err == nil || !strings.Contains(err.Error(), "IPv4") {
		t.Fatalf("an IPv6-only target must be rejected, got %v", err)
	}
	if len(r.lines()) != before {
		t.Fatal("a target that resolves to nothing usable must not run tc")
	}

	// An IP literal never consults the resolver (a nil LookupIP would panic).
	n.LookupIP = nil
	if _, err := n.Apply(context.Background(), "f3", tg, NetemSpec{FaultType: "packet_loss", Target: "10.43.0.9:80", Loss: 1}); err != nil {
		t.Fatal(err)
	}
	if !r.has("match ip dst 10.43.0.9/32 match ip dport 80 0xffff flowid 1:4") {
		t.Fatalf("literal target: filters = %q", r.lines())
	}
}

func TestExecRunner_ErrorCarriesTheCommandAndItsOutput(t *testing.T) {
	_, err := ExecRunner{}.Run(context.Background(), "false", "x")
	if err == nil || !strings.HasPrefix(err.Error(), "false x: exit status 1") {
		t.Fatalf("err = %v, want the command line and exit status", err)
	}
}
