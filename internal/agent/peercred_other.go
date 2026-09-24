//go:build !linux

package agent

import (
	"errors"
	"net"
)

// PeerPodUID is only available on linux; other platforms cannot identify
// the caller and the server refuses every request unless a test injects
// a resolver.
func PeerPodUID(string) PeerPodUIDFunc {
	return func(net.Conn) (string, error) {
		return "", errors.New("agent: peer credentials require linux (SO_PEERCRED)")
	}
}
