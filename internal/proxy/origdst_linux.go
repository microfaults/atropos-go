//go:build linux

package proxy

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

// platformOriginalDst reads the netfilter original destination of a
// REDIRECTed TCP connection (SO_ORIGINAL_DST, IPv4).
func platformOriginalDst(conn net.Conn) (string, error) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return "", fmt.Errorf("proxy: original destination needs a TCP connection, got %T", conn)
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return "", err
	}
	var addr string
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		mreq, e := unix.GetsockoptIPv6Mreq(int(fd), unix.IPPROTO_IP, unix.SO_ORIGINAL_DST)
		if e != nil {
			gerr = fmt.Errorf("proxy: SO_ORIGINAL_DST: %w", e)
			return
		}
		addr, gerr = parseSockaddrIn(mreq.Multiaddr[:])
	}); err != nil {
		return "", err
	}
	return addr, gerr
}
