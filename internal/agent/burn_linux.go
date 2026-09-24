//go:build linux

package agent

import (
	"os/exec"
	"syscall"
)

// setParentDeathSignal ends the stress child when the agent process dies,
// so an agent crash never leaves a burn running in a target cgroup.
func setParentDeathSignal(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGTERM
}
