package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/cpu"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault/resource/memory"
	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// ChildEnv marks a process as the agent's stress child.
const ChildEnv = "ATROPOS_AGENT_STRESS_CHILD"

// childConfig is what the parent sends the child on stdin.
type childConfig struct {
	Fault          wire.FaultRequest `json:"fault"`
	AvailableCPUs  float64           `json:"available_cpus"`
	AvailableBytes uint64            `json:"available_bytes"`
}

// Burn is a running stress child placed in a target cgroup.
type Burn struct {
	cmd  *exec.Cmd
	done chan Result
	pid  int
}

// StartBurn runs `self stress` as a child, moves it into cgroupDir before
// it starts consuming anything, then tells it to go. The child executes the
// existing resource fault implementation with the target's capacity.
func StartBurn(ctx context.Context, self, cgroupDir string, req wire.FaultRequest, avail Capacity) (*Burn, error) {
	cmd := exec.Command(self, "stress")
	setParentDeathSignal(cmd)
	cmd.Env = append(os.Environ(), ChildEnv+"=1")
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("agent: start stress child: %w", err)
	}
	b := &Burn{cmd: cmd, done: make(chan Result, 1), pid: cmd.Process.Pid}
	fail := func(err error) (*Burn, error) {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, err
	}

	cfg, _ := json.Marshal(childConfig{Fault: req, AvailableCPUs: avail.CPUs, AvailableBytes: avail.MemoryBytes})
	if _, err := stdin.Write(append(cfg, '\n')); err != nil {
		return fail(fmt.Errorf("agent: send config to child: %w", err))
	}

	lines := make(chan string, 2)
	readErr := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
		for sc.Scan() {
			lines <- sc.Text()
		}
		readErr <- sc.Err()
		close(lines)
	}()

	select {
	case l, ok := <-lines:
		if !ok || l != "ready" {
			return fail(fmt.Errorf("agent: child did not report ready (got %q)", l))
		}
	case <-time.After(10 * time.Second):
		return fail(errors.New("agent: child did not report ready in time"))
	case <-ctx.Done():
		return fail(ctx.Err())
	}

	// Placement happens while the child is parked on "go": every byte it
	// allocates and every cycle it burns is charged to the target.
	if err := Attach(cgroupDir, b.pid); err != nil {
		return fail(err)
	}
	if _, err := io.WriteString(stdin, "go\n"); err != nil {
		return fail(fmt.Errorf("agent: signal child: %w", err))
	}

	go func() {
		var res *Result
		for l := range lines {
			var r Result
			if json.Unmarshal([]byte(l), &r) == nil {
				res = &r
				break
			}
		}
		for range lines {
		}
		werr := cmd.Wait()
		if res == nil {
			msg := "agent: stress child exited without a result"
			if werr != nil {
				msg += ": " + werr.Error()
			}
			res = &Result{Error: msg}
		}
		b.done <- *res
	}()
	return b, nil
}

// Done delivers the child's result once.
func (b *Burn) Done() <-chan Result { return b.done }

// Stop asks the child to end the fault now (SIGTERM → it cancels its
// context and reports the truncated duration); Kill follows if it does not
// exit.
func (b *Burn) Stop() {
	_ = b.cmd.Process.Signal(syscall.SIGTERM)
	go func() {
		select {
		case <-time.After(10 * time.Second):
			_ = b.cmd.Process.Kill()
		case <-b.done:
		}
	}()
}

// RunStressChild is the child side (`atropos-agent stress`): read the
// config, report ready, wait for go, run the fault, print the result.
// Returns the process exit code.
func RunStressChild(stdin io.Reader, stdout io.Writer) int {
	in := bufio.NewReader(stdin)
	line, err := in.ReadString('\n')
	if err != nil {
		fmt.Fprintln(os.Stderr, "stress child: read config:", err)
		return 2
	}
	var cfg childConfig
	if err := json.Unmarshal([]byte(line), &cfg); err != nil {
		fmt.Fprintln(os.Stderr, "stress child: decode config:", err)
		return 2
	}
	base := fault.FaultConfig{
		Duration: time.Duration(cfg.Fault.DurationMs) * time.Millisecond,
		RampUp:   time.Duration(cfg.Fault.RampUpMs) * time.Millisecond,
		RampDown: time.Duration(cfg.Fault.RampDownMs) * time.Millisecond,
	}
	f, err := instrument.DecodeResourceFault(cfg.Fault.FaultType, cfg.Fault.Params, base)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stress child: decode fault:", err)
		return 2
	}
	switch s := f.(type) {
	case *cpu.Stress:
		s.Config.AvailableCPUs = cfg.AvailableCPUs
	case *memory.Stress:
		s.AvailableBytes = cfg.AvailableBytes
	}

	fmt.Fprintln(stdout, "ready")
	if line, err := in.ReadString('\n'); err != nil || line != "go\n" {
		fmt.Fprintln(os.Stderr, "stress child: expected go, got", line, err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	h, err := f.Start(ctx)
	if err != nil {
		writeResult(stdout, Result{Error: err.Error()})
		return 1
	}
	r := <-h.Done()
	res := Result{ActualDurationMs: r.ActualDuration.Milliseconds(), Detail: r.Detail}
	if r.Err != nil {
		res.Error = r.Err.Error()
	}
	writeResult(stdout, res)
	return 0
}

func writeResult(w io.Writer, r Result) {
	b, _ := json.Marshal(r)
	fmt.Fprintln(w, string(b))
}
