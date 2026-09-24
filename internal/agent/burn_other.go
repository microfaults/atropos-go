//go:build !linux

package agent

import "os/exec"

func setParentDeathSignal(*exec.Cmd) {}
