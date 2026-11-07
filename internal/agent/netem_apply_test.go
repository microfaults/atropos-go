package agent

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
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

// fakeNetnsLink makes <proc>/<pid>/ns/net a link to id (net:[inode]), the
// way the kernel shows a process's network namespace.
func fakeNetnsLink(t *testing.T, proc string, pid int, id string) string {
	t.Helper()
	dir := filepath.Join(proc, strconv.Itoa(pid), "ns")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "net")
	if err := os.Symlink(id, link); err != nil {
		t.Fatal(err)
	}
	return link
}

const (
	hostNetnsID = "net:[4026531840]" // pid 1's: the node's own network namespace
	podNetnsID  = "net:[4026532412]"
	// ourRootQdisc is what `tc qdisc show dev eth0 root` prints while a
	// targeted fault of ours is applied.
	ourRootQdisc = "qdisc prio 1: root refcnt 2 bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1\n"
)

// newTestNetem builds a Netem over a fake /proc (pid 1 in the host
// namespace, pid 4242 in net:[4026532412]), a recording runner (the root
// qdisc probe answers ours) and a resolver answering 10.43.0.5. The
// target's cgroup.procs names pid 4242.
func newTestNetem(t *testing.T) (*Netem, *recRunner, string, Target) {
	t.Helper()
	proc := t.TempDir()
	fakeNetnsLink(t, proc, 1, hostNetnsID)
	netns := fakeNetnsLink(t, proc, 4242, podNetnsID)
	r := &recRunner{out: map[string][]byte{
		"ip -json link show": []byte(ipLinkJSON),
		"tc qdisc show":      []byte(ourRootQdisc),
	}, fail: map[string]error{}}
	n := &Netem{Runner: r, Journal: NetemJournal{Dir: filepath.Join(t.TempDir(), "netem")}, ProcRoot: proc, Logger: slog.New(slog.DiscardHandler),
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

func TestNetem_FailedRootAddRunsNoDel(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["root handle 1: prio"] = errors.New("exit status 2")
	if _, err := n.Apply(context.Background(), "f1", tg, lossSpec); err == nil || !strings.Contains(err.Error(), "exit status 2") {
		t.Fatalf("err = %v, want the root add failure", err)
	}
	if r.has("qdisc del") {
		t.Fatalf("a failed root add must not delete the root qdisc it did not install: %q", r.lines())
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after a failed root add")
	}
	delete(r.fail, "root handle 1: prio")
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
		t.Fatalf("the namespace must be released after a failed root add: %v", err)
	}
}

func TestNetem_FailedLaterLineDeletesTheRootQdisc(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	r.fail["parent 1:4 handle 40: netem"] = errors.New("exit status 2")
	if _, err := n.Apply(context.Background(), "f1", tg, lossSpec); err == nil || !strings.Contains(err.Error(), "exit status 2") {
		t.Fatalf("err = %v, want the netem add failure", err)
	}
	if !r.has("tc qdisc del dev eth0 root") {
		t.Fatal("a failure after the root add must delete the root qdisc again")
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after a rolled-back apply")
	}
}

func TestNetem_FailedRollbackKeepsTheEntryAndTheNamespace(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	n.Logger = slog.New(slog.DiscardHandler)
	r.fail["tc filter add"] = errors.New("exit status 2")
	r.fail["tc qdisc del"] = errors.New("exit status 3")
	_, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err == nil || !strings.Contains(err.Error(), "exit status 2") || !strings.Contains(err.Error(), "rollback: exit status 3") {
		t.Fatalf("err = %v, want the line failure joined with the rollback failure", err)
	}
	es, _ := n.Journal.Load()
	if len(es) != 1 || es[0].ID != "f1" || len(es[0].Applied) != 2 {
		t.Fatalf("journal = %+v, want f1 kept with its two applied lines", es)
	}
	delete(r.fail, "tc filter add")
	delete(r.fail, "tc qdisc del")
	if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); !errors.Is(err, ErrNetemBusy) {
		t.Fatalf("the namespace must stay busy after a failed rollback, got %v", err)
	}
}

func TestNetem_RestoreAllProbesAnEntryWithNoAppliedLine(t *testing.T) {
	for _, c := range []struct {
		name, root string
		wantDel    bool
	}{
		// A crash after the pre-tc save, before the root add ran: the root is the pod's default.
		{"root is the default noqueue", "qdisc noqueue 0: root refcnt 2\n", false},
		// A crash between a successful root add and its journal re-save (the former Ruling 8 residual).
		{"root is our prio 1:", ourRootQdisc, true},
		{"root is our whole-pod netem 1:", "qdisc netem 1: root refcnt 2 limit 1000 loss 10%\n", true},
		{"root is someone else's htb 1:", "qdisc htb 1: root refcnt 2 r2q 10 default 0\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, r, netns, _ := newTestNetem(t)
			r.out["tc qdisc show"] = []byte(c.root)
			if err := n.Journal.Save(NetemEntry{ID: "half", PodUID: "pod-a", Netns: netns, NetnsID: podNetnsID, Dev: "eth0"}); err != nil {
				t.Fatal(err)
			}
			if got := n.RestoreAll(context.Background(), slog.New(slog.DiscardHandler)); got != 1 {
				t.Fatalf("RestoreAll = %d, want 1", got)
			}
			want := []string{"nsenter --net=" + netns + " tc qdisc show dev eth0 root"}
			if c.wantDel {
				want = append(want, "nsenter --net="+netns+" tc qdisc del dev eth0 root")
			}
			if got := r.lines(); strings.Join(got, "\n") != strings.Join(want, "\n") {
				t.Fatalf("commands = %q, want %q", got, want)
			}
			if es, _ := n.Journal.Load(); len(es) != 0 {
				t.Fatal("the entry must be removed")
			}
		})
	}
}

// Critical 1: the journaled pid's link disappears (the app container
// restarted) while pid 4343 (the pause sandbox) still holds the pod's
// namespace: the restore enters the namespace through 4343 and removes the
// qdisc.
func TestNetem_RestoreFindsTheNamespaceThroughAnotherHolder(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(netns); err != nil { // pid 4242 is gone
		t.Fatal(err)
	}
	holder := fakeNetnsLink(t, n.ProcRoot, 4343, podNetnsID)
	before := len(r.lines())
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}
	got := r.lines()[before:]
	if len(got) == 0 || got[len(got)-1] != "nsenter --net="+holder+" tc qdisc del dev eth0 root" {
		t.Fatalf("restore ran %q, want the root qdisc deleted through pid 4343", got)
	}
	for _, l := range got {
		if strings.Contains(l, netns) {
			t.Fatalf("restore must not use the vanished pid's path: %q", l)
		}
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("journal must be empty after the restore")
	}
	if _, err := n.Apply(context.Background(), "f2", Target{PodUID: "pod-a", CgroupDir: cgroupDir(t, map[string]string{"cgroup.procs": "4343\n"})}, lossSpec); err != nil {
		t.Fatalf("the namespace must be free after the restore: %v", err)
	}
}

// With no process holding the journaled inode the namespace is gone: the
// entry is dropped without tc.
func TestNetem_RestoreDropsAnEntryWhoseNamespaceNobodyHolds(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	os.Remove(netns)
	before := len(r.lines())
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatalf("restore of a vanished namespace must succeed: %v", err)
	}
	if len(r.lines()) != before {
		t.Fatalf("no tc may run for a namespace nobody holds, ran %q", r.lines()[before:])
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("the stale entry must be dropped")
	}
}

// Critical 2: a stale entry (kept by an earlier agent run) whose namespace a
// newer fault now holds is dropped without tc: the old root qdisc cannot
// coexist with the newer fault's, so deleting the root would remove the
// newer fault.
func TestNetem_RestoreDropsAnEntrySupersededByANewerFault(t *testing.T) {
	n, r, netns, tg := newTestNetem(t)
	old := NetemEntry{ID: "old", PodUID: "pod-a", Netns: netns, NetnsID: podNetnsID, Dev: "eth0",
		Applied: []string{"tc qdisc add dev eth0 root handle 1: netem loss 5%"}}
	if err := n.Journal.Save(old); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Apply(context.Background(), "new", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	before := len(r.lines())
	if err := n.Restore(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	if len(r.lines()) != before {
		t.Fatalf("a superseded entry must run no tc, ran %q", r.lines()[before:])
	}
	es, _ := n.Journal.Load()
	if len(es) != 1 || es[0].ID != "new" {
		t.Fatalf("journal = %+v, want only the newer fault", es)
	}
	if _, err := n.Apply(context.Background(), "third", tg, lossSpec); !errors.Is(err, ErrNetemBusy) {
		t.Fatalf("the newer fault must still hold the namespace, got %v", err)
	}
}

// An entry whose journal file is already gone (another restore finished it)
// is not restored again.
func TestNetem_RestoreOfAnAlreadyRemovedEntryRunsNoTC(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}
	before := len(r.lines())
	if err := n.Restore(context.Background(), *e); err != nil {
		t.Fatal(err)
	}
	if len(r.lines()) != before {
		t.Fatalf("a second restore of the same entry must run no tc, ran %q", r.lines()[before:])
	}
}

// Minor 8: a boot restore that fails keeps the entry AND reserves its
// namespace, so a later Apply there answers ErrNetemBusy (409), not tc's
// "File exists".
func TestNetem_RestoreAllReservesTheNamespaceOfAKeptEntry(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	if _, err := n.Apply(context.Background(), "crashed", tg, lossSpec); err != nil {
		t.Fatal(err)
	}
	r.fail["tc qdisc del"] = errors.New("exit status 2")
	boot := &Netem{Runner: r, Journal: n.Journal, ProcRoot: n.ProcRoot, LookupIP: n.LookupIP}
	if got := boot.RestoreAll(context.Background(), slog.New(slog.DiscardHandler)); got != 0 {
		t.Fatalf("RestoreAll = %d, want 0 (the del failed)", got)
	}
	if es, _ := n.Journal.Load(); len(es) != 1 {
		t.Fatalf("journal = %+v, want the entry kept", es)
	}
	before := len(r.lines())
	if _, err := boot.Apply(context.Background(), "f2", tg, lossSpec); !errors.Is(err, ErrNetemBusy) {
		t.Fatalf("Apply over a kept entry: err = %v, want ErrNetemBusy", err)
	}
	if len(r.lines()) != before {
		t.Fatal("a refused apply must run no command")
	}
}

// Important 3: a restore whose root qdisc is no longer ours (removed by
// hand, or by a restore that crashed before its journal removal) is already
// restored: no del runs, the entry is removed and the namespace freed.
func TestNetem_RestoreIsIdempotent(t *testing.T) {
	for _, c := range []struct {
		name string
		set  func(r *recRunner)
	}{
		{"root is noqueue", func(r *recRunner) { r.out["tc qdisc show"] = []byte("qdisc noqueue 0: root refcnt 2\n") }},
		{"device is gone", func(r *recRunner) { r.fail["tc qdisc show"] = errors.New(`exit status 1: Cannot find device "eth0"`) }},
	} {
		t.Run(c.name, func(t *testing.T) {
			n, r, netns, tg := newTestNetem(t)
			e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
			if err != nil {
				t.Fatal(err)
			}
			c.set(r)
			before := len(r.lines())
			if err := n.Restore(context.Background(), *e); err != nil {
				t.Fatal(err)
			}
			if got := r.lines()[before:]; len(got) != 1 || got[0] != "nsenter --net="+netns+" tc qdisc show dev eth0 root" {
				t.Fatalf("commands = %q, want the probe only", got)
			}
			if es, _ := n.Journal.Load(); len(es) != 0 {
				t.Fatal("the entry must be removed")
			}
			delete(r.fail, "tc qdisc show")
			if _, err := n.Apply(context.Background(), "f2", tg, lossSpec); err != nil {
				t.Fatalf("the namespace must be free: %v", err)
			}
		})
	}
}

// A probe that fails for another reason keeps the entry: nothing is known.
func TestNetem_RestoreKeepsTheEntryWhenTheProbeFails(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	e, err := n.Apply(context.Background(), "f1", tg, lossSpec)
	if err != nil {
		t.Fatal(err)
	}
	r.fail["tc qdisc show"] = errors.New("exit status 1: nsenter: cannot open: Permission denied")
	if err := n.Restore(context.Background(), *e); err == nil || !strings.Contains(err.Error(), "Permission denied") {
		t.Fatalf("err = %v, want the probe's failure", err)
	}
	if r.has("qdisc del") {
		t.Fatal("no del may run without a probe answer")
	}
	if es, _ := n.Journal.Load(); len(es) != 1 {
		t.Fatal("the entry must be kept")
	}
}

// Important 5: a target in the host's network namespace (hostNetwork: true)
// is refused before any tc: the fault would shape the whole node.
func TestNetem_RefusesAHostNetworkPod(t *testing.T) {
	n, r, _, tg := newTestNetem(t)
	os.Remove(filepath.Join(n.ProcRoot, "1", "ns", "net"))
	fakeNetnsLink(t, n.ProcRoot, 1, podNetnsID) // pid 1 shares the target's namespace
	if _, err := n.Apply(context.Background(), "f1", tg, lossSpec); !errors.Is(err, ErrNetemHostNetwork) {
		t.Fatalf("err = %v, want ErrNetemHostNetwork", err)
	}
	if len(r.lines()) != 0 {
		t.Fatalf("a refused apply must run nothing, ran %q", r.lines())
	}
	if es, _ := n.Journal.Load(); len(es) != 0 {
		t.Fatal("a refused apply must journal nothing")
	}
}
