package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// OriginalDstFunc resolves the destination a redirected connection was
// originally opened to ("ip:port") before iptables REDIRECTed it to the
// sidecar. The platform implementation reads SO_ORIGINAL_DST; tests inject
// one.
type OriginalDstFunc func(net.Conn) (string, error)

// ErrOriginalDstUnsupported is returned on platforms without netfilter.
var ErrOriginalDstUnsupported = errors.New("proxy: original destination lookup requires linux (SO_ORIGINAL_DST)")

const afInet = 2

// parseSockaddrIn decodes the sockaddr_in that SO_ORIGINAL_DST returns:
// sa_family (native endian), sin_port (network order), sin_addr. IPv6
// original destinations (IP6T_SO_ORIGINAL_DST, sockaddr_in6) are a
// follow-up; the lab cluster is IPv4.
func parseSockaddrIn(b []byte) (string, error) {
	if len(b) < 8 {
		return "", fmt.Errorf("proxy: sockaddr too short (%d bytes)", len(b))
	}
	if family := binary.NativeEndian.Uint16(b[0:2]); family != afInet {
		return "", fmt.Errorf("proxy: original destination family %d is not AF_INET (IPv6 is not supported yet)", family)
	}
	port := binary.BigEndian.Uint16(b[2:4])
	ip := net.IPv4(b[4], b[5], b[6], b[7])
	return net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), nil
}

// origDstConn carries the original destination of an accepted connection
// so http.Server.ConnContext can attach it to every request on it.
type origDstConn struct {
	net.Conn
	dst string
	err error
}

type origDstKey struct{}

// connContext is the http.Server.ConnContext for transparent listeners.
func connContext(ctx context.Context, c net.Conn) context.Context {
	if oc, ok := c.(*origDstConn); ok {
		return context.WithValue(ctx, origDstKey{}, oc)
	}
	return ctx
}

func origDstFrom(ctx context.Context) (string, error) {
	oc, _ := ctx.Value(origDstKey{}).(*origDstConn)
	if oc == nil {
		return "", errors.New("connection was not accepted by a transparent listener")
	}
	return oc.dst, oc.err
}

// NewTransparentTransport returns the base transport for transparent
// egress: it dials the request's original destination (from the
// connection the app opened) instead of resolving URL.Host, so the proxy
// reaches exactly the endpoint the app targeted while URL.Host stays the
// name the app used — which is what canonical_v2 keys fold, identically to
// the in-process host. Requests without an original destination in their
// context (the explicit listeners, direct use) dial addr as usual.
func NewTransparentTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if dst, err := origDstFrom(ctx); err == nil && dst != "" {
			addr = dst
		}
		return dialer.DialContext(ctx, network, addr)
	}
	return t
}
