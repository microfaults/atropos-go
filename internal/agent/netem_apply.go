package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Runner runs an external command and returns its combined output. The
// agent runs nsenter, ip and tc through it so tests can record the command
// lines instead of executing them.
type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

// ExecRunner runs commands with os/exec.
type ExecRunner struct{}

// Run implements Runner. A failure carries the command line and its trimmed
// output, so tc's own message reaches the caller.
func (ExecRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return out, nil
}

// RunnerFunc adapts a function to Runner.
type RunnerFunc func(ctx context.Context, name string, args ...string) ([]byte, error)

// Run implements Runner.
func (f RunnerFunc) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return f(ctx, name, args...)
}

// NetemEntry is one applied netem fault, journaled before its first tc
// command runs so it can be removed after a stop, a crash or an agent
// restart. Netns is <proc>/<pid>/ns/net of the process the agent entered;
// NetnsID is that link's target at apply time (net:[inode]), the
// namespace's identity. A later restore enters the namespace by that
// identity, not by the path: Netns while it still links to NetnsID, else
// any live process that holds the inode (see resolveNetns), so an app
// container restart mid-fault does not strand the qdisc. Applied
// holds the tc lines (space-joined argv) that succeeded, in the order they
// ran: the entry is saved with none before the first tc line and re-saved
// after each one. A restore does not trust Applied to decide whether our
// root qdisc exists: it probes `tc qdisc show` for our handle 1:, so an
// agent crash between a successful root add and that re-save (an entry
// with no applied line over our root) is still restored.
type NetemEntry struct {
	ID        string    `json:"id"`
	PodUID    string    `json:"pod_uid"`
	Netns     string    `json:"netns"`
	NetnsID   string    `json:"netns_id"`
	Dev       string    `json:"dev"`
	Applied   []string  `json:"applied"`
	AppliedAt time.Time `json:"applied_at"`
}

// NetemJournal persists applied netem faults as one JSON file each, in the
// same layout as Journal.
type NetemJournal struct{ Dir string }

// Save writes e atomically.
func (j NetemJournal) Save(e NetemEntry) error { return writeJournalFile(j.Dir, e.ID, e) }

// Remove deletes the journal entry of id.
func (j NetemJournal) Remove(id string) error { return removeJournalFile(j.Dir, id) }

// Load reads every journaled netem fault.
func (j NetemJournal) Load() ([]NetemEntry, error) {
	files, err := readJournalFiles(j.Dir)
	if err != nil {
		return nil, err
	}
	var out []NetemEntry
	for _, b := range files {
		var e NetemEntry
		if json.Unmarshal(b, &e) == nil && e.ID != "" && e.Netns != "" {
			out = append(out, e)
		}
	}
	return out, nil
}

// ErrNetemBusy refuses a second netem fault in a network namespace that
// already carries one: each fault owns the interface's root qdisc.
var ErrNetemBusy = errors.New("agent: a netem fault is already applied in this pod's network namespace")

// ErrNetemHostNetwork refuses a netem fault on a pod that shares the host's
// network namespace (hostNetwork: true): its egress interface is the
// node's, so the fault would shape every pod on the node, kubelet and the
// agent's own traffic included.
var ErrNetemHostNetwork = errors.New("netem: target pod shares the host network namespace")

// netemRollbackTimeout bounds the best-effort qdisc removal after a failed
// apply; it runs on a fresh context so a cancelled caller still cleans up.
const netemRollbackTimeout = 10 * time.Second

// Netem applies and restores tc netem faults inside pods' network
// namespaces. At most one fault is active per namespace.
type Netem struct {
	Runner  Runner
	Journal NetemJournal
	// ProcRoot is the host's /proc ("" means /proc).
	ProcRoot string
	// LookupIP resolves a target host (nil means net.DefaultResolver.LookupIPAddr).
	LookupIP func(ctx context.Context, host string) ([]net.IPAddr, error)
	// Logger receives Apply's rollback warnings (nil means slog.Default()).
	Logger *slog.Logger

	mu        sync.Mutex
	active    map[string]string // netns id → fault id holding it
	restoring map[string]bool   // fault ids whose restore is running (single-flight)
}

func (n *Netem) procRoot() string {
	if n.ProcRoot == "" {
		return "/proc"
	}
	return n.ProcRoot
}

func (n *Netem) logger() *slog.Logger {
	if n.Logger == nil {
		return slog.Default()
	}
	return n.Logger
}

func (n *Netem) reserve(netnsID, id string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.active[netnsID] != "" {
		return ErrNetemBusy
	}
	if n.active == nil {
		n.active = map[string]string{}
	}
	n.active[netnsID] = id
	return nil
}

// release frees netnsID if id still holds it.
func (n *Netem) release(netnsID, id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.active[netnsID] == id {
		delete(n.active, netnsID)
	}
}

// Apply installs spec in the network namespace of t's first process. The
// order is the contract: read the namespace link; refuse a namespace that
// is the host's (pid 1's: a hostNetwork pod) with ErrNetemHostNetwork, and
// fail when pid 1's link cannot be read; reserve the namespace
// under mu (ErrNetemBusy when taken), released again on every failure
// below; resolve the target to its IPv4 addresses; pick the egress
// interface (eth0 when the listing fails or names none); build the plan and
// journal the entry (no applied line yet) BEFORE the first tc command, so a
// crash mid-apply is restorable; run the plan's lines in order, re-saving
// the entry with each line that succeeded. On the first failure (a line, or
// the re-save after it):
//   - nothing applied yet (the root add failed): no tc del runs, since the
//     root qdisc there is not ours; the entry is removed, the namespace
//     released and the error returned;
//   - our root qdisc exists: tc qdisc del dev <dev> root runs on a fresh
//     context. When it succeeds, the entry is removed, the namespace
//     released and the error returned; when it fails, the qdisc may still
//     be applied, so the entry is kept and the namespace stays busy (like a
//     failed Restore: RestoreAll or a later stop retries), and the error
//     returned is "<err>; rollback: <del err>" wrapping both.
//
// A failing journal removal is logged at Warn.
func (n *Netem) Apply(ctx context.Context, id string, t Target, spec NetemSpec) (*NetemEntry, error) {
	pid, err := FirstPID(t.CgroupDir)
	if err != nil {
		return nil, err
	}
	netns := filepath.Join(n.procRoot(), strconv.Itoa(pid), "ns", "net")
	netnsID, err := os.Readlink(netns)
	if err != nil {
		return nil, fmt.Errorf("agent: network namespace of pid %d (does the agent run with hostPID?): %w", pid, err)
	}
	hostID, err := os.Readlink(filepath.Join(n.procRoot(), "1", "ns", "net"))
	if err != nil {
		return nil, fmt.Errorf("agent: host network namespace (pid 1; does the agent run with hostPID?): %w", err)
	}
	if hostID == netnsID {
		return nil, ErrNetemHostNetwork
	}
	if err := n.reserve(netnsID, id); err != nil {
		return nil, err
	}

	ips, port, err := n.resolveTarget(ctx, spec.Target)
	if err != nil {
		n.release(netnsID, id)
		return nil, err
	}

	nsArg := "--net=" + netns
	dev := ""
	if out, err := n.Runner.Run(ctx, "nsenter", nsArg, "ip", "-json", "link", "show"); err == nil {
		dev = pickInterface(out)
	}
	if dev == "" {
		dev = "eth0"
	}

	plan := netemPlan(dev, ips, port, spec)
	e := &NetemEntry{ID: id, PodUID: t.PodUID, Netns: netns, NetnsID: netnsID, Dev: dev, AppliedAt: time.Now()}
	if err := n.Journal.Save(*e); err != nil {
		n.release(netnsID, id)
		return nil, fmt.Errorf("agent: journal netem: %w", err)
	}

	for _, line := range plan {
		if _, err := n.Runner.Run(ctx, "nsenter", append([]string{nsArg}, line...)...); err != nil {
			return nil, n.rollback(e, err)
		}
		e.Applied = append(e.Applied, strings.Join(line, " "))
		if err := n.Journal.Save(*e); err != nil {
			return nil, n.rollback(e, fmt.Errorf("agent: journal netem: %w", err))
		}
	}
	return e, nil
}

// rollback undoes a failed Apply of e (see Apply) and returns the error to
// report. Only once a line succeeded is there a root qdisc of ours to
// delete: when the root add itself failed, the root qdisc (if any) belongs
// to someone else, e.g. the CNI, and is left alone.
func (n *Netem) rollback(e *NetemEntry, cause error) error {
	if len(e.Applied) > 0 {
		rctx, cancel := context.WithTimeout(context.Background(), netemRollbackTimeout)
		_, delErr := n.Runner.Run(rctx, "nsenter", "--net="+e.Netns, "tc", "qdisc", "del", "dev", e.Dev, "root")
		cancel()
		if delErr != nil {
			n.logger().Warn("agent: netem rollback failed; journal entry kept", "id", e.ID, "dev", e.Dev, "netns", e.Netns, "error", delErr)
			return fmt.Errorf("%w; rollback: %w", cause, delErr)
		}
	}
	if err := n.Journal.Remove(e.ID); err != nil {
		n.logger().Warn("agent: remove netem journal entry", "id", e.ID, "dev", e.Dev, "netns", e.Netns, "error", err)
	}
	n.release(e.NetnsID, e.ID)
	return cause
}

// resolveTarget turns a host[:port] target into its IPv4 addresses and the
// port (0 when none). An empty target means the whole pod: no addresses.
func (n *Netem) resolveTarget(ctx context.Context, target string) ([]net.IP, int, error) {
	if target == "" {
		return nil, 0, nil
	}
	host, port := target, 0
	if h, p, err := net.SplitHostPort(target); err == nil {
		host = h
		port, err = strconv.Atoi(p)
		if err != nil {
			return nil, 0, fmt.Errorf("agent: target %q: malformed port: %w", target, err)
		}
	}
	var addrs []net.IP
	if ip := net.ParseIP(host); ip != nil {
		addrs = []net.IP{ip}
	} else {
		lookup := n.LookupIP
		if lookup == nil {
			lookup = net.DefaultResolver.LookupIPAddr
		}
		res, err := lookup(ctx, host)
		if err != nil {
			return nil, 0, fmt.Errorf("agent: resolve target %q: %w", target, err)
		}
		for _, a := range res {
			addrs = append(addrs, a.IP)
		}
	}
	var ips []net.IP
	for _, ip := range addrs {
		if v4 := ip.To4(); v4 != nil {
			ips = append(ips, v4)
		}
	}
	if len(ips) == 0 {
		return nil, 0, fmt.Errorf("agent: target %q resolved to no IPv4 address", target)
	}
	return ips, port, nil
}

// Restore removes e's root qdisc and its journal entry. It is
// single-flight and checked, under mu, before any tc runs:
//   - a restore of e.ID already under way: nil at once (the running one
//     finishes the entry);
//   - e's journal file is gone: another restore finished it, nil;
//   - e's namespace is reserved by another fault id: e is stale (its root
//     qdisc cannot coexist with the newer fault's root), so its entry is
//     dropped without tc and the newer fault is left alone, nil;
//   - otherwise the namespace is reserved for e.ID (a boot restore holds it
//     too) and e is marked in flight.
//
// tc then runs with mu released. It enters the namespace named by
// e.NetnsID through resolveNetns (e.Netns, or another live process holding
// the same inode); a namespace no process holds is gone: it is never
// touched and its entry is dropped. Restore is idempotent: it probes the
// root qdisc first (rootQdiscIsOurs) and runs tc qdisc del dev <dev> root
// only when the root is ours (prio or netem, handle 1:). A root that is not
// ours (already removed by an earlier restore that crashed before its
// journal removal, or by hand; or the device is gone) means the entry is
// already restored: it is dropped and logged at Info. The probe decides
// for an entry with no applied line too, so a crash between a successful
// root add and its journal re-save is restored, not left behind. A probe
// that fails otherwise keeps the entry. Finally, under mu again: a
// successful restore removes the entry and frees the namespace; a failing
// tc keeps both (a later restore retries; a new Apply there gets
// ErrNetemBusy) and returns the error.
func (n *Netem) Restore(ctx context.Context, e NetemEntry) error {
	o, err := n.restore(ctx, e)
	if err == nil && o == alreadyRestored {
		n.logger().Info("agent: netem root qdisc already removed; entry dropped", "id", e.ID, "dev", e.Dev, "netns_id", e.NetnsID)
	}
	return err
}

// resolveNetns finds a path that enters the network namespace netnsID
// (net:[inode]) now. The journaled path (the app container's first pid) is
// only a hint: the pod's namespace belongs to its sandbox and outlives that
// process, so a container restart leaves the namespace without the pid. It
// returns hint while hint still links to netnsID; otherwise the
// <proc>/<pid>/ns/net of the first live process whose link equals netnsID
// (the pause process always does while the pod lives). ok is false only
// when no process holds the inode: the namespace is gone.
func (n *Netem) resolveNetns(hint, netnsID string) (path string, ok bool) {
	if cur, err := os.Readlink(hint); err == nil && cur == netnsID {
		return hint, true
	}
	ents, err := os.ReadDir(n.procRoot())
	if err != nil {
		return "", false
	}
	for _, d := range ents {
		if _, err := strconv.Atoi(d.Name()); err != nil {
			continue
		}
		p := filepath.Join(n.procRoot(), d.Name(), "ns", "net")
		if cur, err := os.Readlink(p); err == nil && cur == netnsID {
			return p, true
		}
	}
	return "", false
}

// restoreOutcome says what restore did with an entry.
type restoreOutcome int

const (
	restoredNetem   restoreOutcome = iota // tc qdisc del ran
	netnsGone                             // no process holds the namespace: dropped
	neverApplied                          // no applied line and the root is not ours: dropped without a del
	alreadyRestored                       // the root qdisc is not ours (already removed): dropped without a del
	restoreInFlight                       // another restore of the id is running: nothing done
	entryGone                             // the journal file is already removed: nothing done
	superseded                            // another fault holds the namespace: dropped without tc
)

// restore is Restore, also reporting what it did.
func (n *Netem) restore(ctx context.Context, e NetemEntry) (restoreOutcome, error) {
	if o, done, err := n.beginRestore(e); done {
		return o, err
	}
	outcome, err := n.restoreTC(ctx, e)
	if ferr := n.finishRestore(e, err == nil); err == nil {
		err = ferr
	}
	return outcome, err
}

// beginRestore runs restore's checks under mu (see Restore). done reports
// that restore is over: o (and the error of dropping a superseded entry)
// is its result. Otherwise e's namespace is now reserved for e.ID and e.ID
// is in flight until finishRestore.
func (n *Netem) beginRestore(e NetemEntry) (o restoreOutcome, done bool, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.restoring[e.ID] {
		return restoreInFlight, true, nil
	}
	if _, err := os.Stat(journalPath(n.Journal.Dir, e.ID)); errors.Is(err, os.ErrNotExist) {
		return entryGone, true, nil
	}
	if holder := n.active[e.NetnsID]; holder != "" && holder != e.ID {
		return superseded, true, n.Journal.Remove(e.ID)
	}
	if n.active == nil {
		n.active = map[string]string{}
	}
	if n.restoring == nil {
		n.restoring = map[string]bool{}
	}
	n.active[e.NetnsID] = e.ID
	n.restoring[e.ID] = true
	return 0, false, nil
}

// finishRestore ends the restore of e under mu: when it succeeded, the
// entry is removed (its error returned) and the namespace freed; when it
// failed, both stay.
func (n *Netem) finishRestore(e NetemEntry, ok bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.restoring, e.ID)
	if !ok {
		return nil
	}
	err := n.Journal.Remove(e.ID)
	if n.active[e.NetnsID] == e.ID {
		delete(n.active, e.NetnsID)
	}
	return err
}

// restoreTC is restore's part outside mu: the namespace lookup, the root
// qdisc probe and the del. A nil error means the entry is finished
// (restored, or dropped).
func (n *Netem) restoreTC(ctx context.Context, e NetemEntry) (restoreOutcome, error) {
	netns, ok := n.resolveNetns(e.Netns, e.NetnsID)
	if !ok {
		return netnsGone, nil
	}
	ours, err := n.rootQdiscIsOurs(ctx, netns, e.Dev)
	if err != nil {
		return restoredNetem, err
	}
	if !ours {
		if len(e.Applied) == 0 {
			return neverApplied, nil
		}
		return alreadyRestored, nil
	}
	if _, err := n.Runner.Run(ctx, "nsenter", "--net="+netns, "tc", "qdisc", "del", "dev", e.Dev, "root"); err != nil {
		return restoredNetem, err
	}
	return restoredNetem, nil
}

// rootQdiscIsOurs probes `tc qdisc show dev <dev> root` inside netns and
// reports whether the root qdisc is one an Apply installs: prio or netem
// with handle 1:. The pod's default (noqueue, fq_codel, handle 0:), another
// owner's qdisc, or a device that no longer exists ("Cannot find device")
// is not ours. Any other probe failure is returned: nothing is known.
func (n *Netem) rootQdiscIsOurs(ctx context.Context, netns, dev string) (bool, error) {
	out, err := n.Runner.Run(ctx, "nsenter", "--net="+netns, "tc", "qdisc", "show", "dev", dev, "root")
	if err != nil {
		if strings.Contains(err.Error(), "Cannot find device") || bytes.Contains(out, []byte("Cannot find device")) {
			return false, nil
		}
		return false, fmt.Errorf("agent: probe root qdisc of %s: %w", dev, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) >= 3 && f[0] == "qdisc" && (f[1] == "prio" || f[1] == "netem") && f[2] == "1:" {
			return true, nil
		}
	}
	return false, nil
}

// RestoreAll restores every journaled netem fault (agent boot): a netem
// fault must never outlive the agent that applied it. Returns how many
// entries were restored or discarded (namespace gone, root not ours, or
// superseded); every entry, one with no applied line included, is probed. An entry whose restore fails is kept and its namespace
// stays reserved, so an Apply there answers ErrNetemBusy until a DELETE of
// the id retries the restore.
func (n *Netem) RestoreAll(ctx context.Context, logger *slog.Logger) int {
	es, err := n.Journal.Load()
	if err != nil {
		logger.Warn("agent: read netem journal", "error", err)
		return 0
	}
	count := 0
	for _, e := range es {
		outcome, err := n.restore(ctx, e)
		if err != nil {
			logger.Warn("agent: restore journaled netem", "id", e.ID, "netns", e.Netns, "dev", e.Dev, "error", err)
			continue
		}
		count++
		switch outcome {
		case netnsGone:
			logger.Info("agent: journaled netem namespace gone", "id", e.ID, "netns", e.Netns, "netns_id", e.NetnsID)
		case neverApplied:
			logger.Info("agent: journaled netem never applied", "id", e.ID, "netns", e.Netns)
		case alreadyRestored:
			logger.Info("agent: journaled netem already removed", "id", e.ID, "netns", e.Netns, "dev", e.Dev)
		case superseded:
			logger.Info("agent: journaled netem superseded by a newer fault in its namespace", "id", e.ID, "netns_id", e.NetnsID)
		case entryGone, restoreInFlight:
			logger.Info("agent: journaled netem already being restored", "id", e.ID)
		default:
			logger.Info("agent: restored journaled netem", "id", e.ID, "netns", e.Netns, "dev", e.Dev)
		}
	}
	return count
}
