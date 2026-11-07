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
// NetnsID is that link's target at apply time (net:[inode]), which tells a
// later restore whether the path still names the same namespace. Applied
// holds the tc lines (space-joined argv) that succeeded, in the order they
// ran: the entry is saved with none before the first tc line and re-saved
// after each one, so an entry with no applied line installed nothing of
// ours and is dropped without a tc command. The known exception: an agent
// crash between a successful root add and that re-save leaves our root
// qdisc behind an entry with no applied line; it is removed by hand (see
// deploy/agent/README.md).
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

	mu     sync.Mutex
	active map[string]string // netns id → fault id
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
// order is the contract: read the namespace link; reserve the namespace
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

// Restore removes e's root qdisc and its journal entry. It trusts the
// journal only while e.Netns still links to e.NetnsID: a namespace that is
// gone or whose path now names another namespace is never touched, its
// entry is dropped and nil returned. An entry with no applied line (its
// root add never succeeded) installed nothing of ours: it is dropped the
// same way, without tc (except after an agent crash between a successful
// root add and its journal re-save, which leaves our root qdisc for manual
// removal; see deploy/agent/README.md). A failing tc keeps the entry (a
// later restore can retry) and returns the error.
func (n *Netem) Restore(ctx context.Context, e NetemEntry) error {
	_, err := n.restore(ctx, e)
	return err
}

// restoreOutcome says what restore did with an entry.
type restoreOutcome int

const (
	restoredNetem restoreOutcome = iota // tc qdisc del ran
	netnsGone                           // namespace gone or reused: dropped
	neverApplied                        // no applied line: dropped
)

// restore is Restore, also reporting what it did.
func (n *Netem) restore(ctx context.Context, e NetemEntry) (restoreOutcome, error) {
	drop := func(o restoreOutcome) (restoreOutcome, error) {
		err := n.Journal.Remove(e.ID)
		n.release(e.NetnsID, e.ID)
		return o, err
	}
	if len(e.Applied) == 0 {
		return drop(neverApplied)
	}
	if cur, err := os.Readlink(e.Netns); err != nil || cur != e.NetnsID {
		return drop(netnsGone)
	}
	if _, err := n.Runner.Run(ctx, "nsenter", "--net="+e.Netns, "tc", "qdisc", "del", "dev", e.Dev, "root"); err != nil {
		return restoredNetem, err
	}
	return drop(restoredNetem)
}

// RestoreAll restores every journaled netem fault (agent boot): a netem
// fault must never outlive the agent that applied it. Returns how many
// entries were restored or discarded (namespace gone, or never applied).
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
		default:
			logger.Info("agent: restored journaled netem", "id", e.ID, "netns", e.Netns, "dev", e.Dev)
		}
	}
	return count
}
