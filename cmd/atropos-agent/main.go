// Command atropos-agent is the node agent that executes resource faults for
// sidecar-instrumented pods inside the target container's cgroup, and netem
// network faults (tc, through nsenter) inside the target pod's network
// namespace (design doc §5.14). It runs as a privileged DaemonSet with
// hostPID and the host cgroup filesystem mounted, listens on a Unix socket
// sidecars mount, and refuses any request for a pod other than the caller's.
//
//	atropos-agent [serve] [-sock PATH] [-cgroup-root DIR] [-task-root DIR]... [-journal DIR] [-netem-journal DIR] [-proc-root DIR]
//	atropos-agent stress            # internal: the placed stress child
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"git.ucsc.edu/microfaults/atropos-go/internal/agent"
)

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "stress" {
		os.Exit(agent.RunStressChild(os.Stdin, os.Stdout))
	}
	if len(args) > 0 && args[0] == "serve" {
		args = args[1:]
	}
	fs := flag.NewFlagSet("atropos-agent", flag.ExitOnError)
	sock := fs.String("sock", agent.DefaultSocket, "unix socket sidecars connect to")
	cgroupRoot := fs.String("cgroup-root", agent.DefaultCgroupRoot, "host cgroup v2 root as mounted in this container")
	var taskRoots multiFlag
	fs.Var(&taskRoots, "task-root", "containerd task root holding <root>/k8s.io/<id>/config.json (repeatable; default: upstream and k3s paths)")
	journal := fs.String("journal", "/var/lib/atropos-agent/quotas", "directory journaling applied quotas for restore")
	netemJournal := fs.String("netem-journal", "/var/lib/atropos-agent/netem", "directory journaling applied netem faults for restore")
	procRoot := fs.String("proc-root", "/proc", "proc filesystem used to identify callers (host pid namespace)")
	_ = fs.Parse(args)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	self, err := os.Executable()
	if err != nil {
		self = os.Args[0]
	}
	j := agent.Journal{Dir: *journal}
	if n := agent.RestoreAll(j, logger); n > 0 {
		logger.Warn("atropos-agent: restored quotas left by a previous run", "count", n)
	}
	nt := &agent.Netem{Runner: agent.ExecRunner{}, Journal: agent.NetemJournal{Dir: *netemJournal}, ProcRoot: *procRoot, Logger: logger}
	for _, bin := range []string{"tc", "nsenter"} {
		if _, err := exec.LookPath(bin); err != nil {
			logger.Warn("atropos-agent: netem not offered: binary missing", "binary", bin, "error", err)
			nt = nil
			break
		}
	}
	if nt != nil {
		if n := nt.RestoreAll(ctx, logger); n > 0 {
			logger.Warn("atropos-agent: restored netem faults left by a previous run", "count", n)
		}
	}
	srv := &agent.Server{
		Resolver: &agent.Resolver{TaskRoots: taskRoots, CgroupRoot: *cgroupRoot},
		Journal:  j,
		Self:     self,
		Peer:     agent.PeerPodUID(*procRoot),
		Host:     agent.HostCapacity(*procRoot),
		Logger:   logger,
		Netem:    nt,
	}
	ln, err := agent.Listen(*sock)
	if err != nil {
		fmt.Fprintln(os.Stderr, "atropos-agent: listen:", err)
		os.Exit(1)
	}
	logger.Info("atropos-agent ready", "socket", *sock, "cgroup_root", *cgroupRoot, "task_roots", taskRoots, "journal", *journal,
		"netem_journal", *netemJournal, "mechanisms", srv.Mechanisms(), "host_cpus", srv.Host.CPUs, "host_memory", srv.Host.MemoryBytes)
	if err := srv.Serve(ctx, ln); err != nil {
		logger.Error("atropos-agent: exit", "error", err)
		os.Exit(1)
	}
}
