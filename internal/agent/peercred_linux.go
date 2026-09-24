//go:build linux

package agent

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// PeerPodUID resolves the caller's pod through SO_PEERCRED and the host's
// /proc (the agent runs with hostPID).
func PeerPodUID(procRoot string) PeerPodUIDFunc {
	return func(c net.Conn) (string, error) {
		uc, ok := c.(*net.UnixConn)
		if !ok {
			return "", fmt.Errorf("agent: peer credentials need a unix socket, got %T", c)
		}
		raw, err := uc.SyscallConn()
		if err != nil {
			return "", err
		}
		var cred *unix.Ucred
		var gerr error
		if err := raw.Control(func(fd uintptr) {
			cred, gerr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
		}); err != nil {
			return "", err
		}
		if gerr != nil {
			return "", fmt.Errorf("agent: SO_PEERCRED: %w", gerr)
		}
		return podUIDFromProc(procRoot, int(cred.Pid))
	}
}
