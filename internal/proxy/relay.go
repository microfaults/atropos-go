package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
)

const relayDialTimeout = 5 * time.Second

// relay carries one raw-TCP connection (a non-HTTP dependency such as
// redis) to its original destination, applying an armed toxic policy for
// that destination the way the TCP proxy fault does: a stream toxic on the
// chosen direction, rst by SetLinger(0) on both ends once it triggers,
// blackhole by never dialing. Either side closing, or the policy ending,
// closes both ends (a stopped fault must not keep shaping traffic).
func relay(ctx context.Context, client net.Conn, dst string, table *PolicyTable, logger *slog.Logger) {
	defer client.Close()

	var a *armed
	if table != nil {
		a, _ = table.matchAddr(dst)
	}
	if a != nil {
		if _, ok := a.Toxic.(*network.Blackhole); ok {
			gone := make(chan struct{})
			go func() {
				io.Copy(io.Discard, client)
				close(gone)
			}()
			select {
			case <-a.ctx.Done():
			case <-gone:
			case <-ctx.Done():
			}
			return
		}
	}

	up, err := net.DialTimeout("tcp", dst, relayDialTimeout)
	if err != nil {
		logger.Warn("atroposd: raw relay dial failed", "dst", dst, "error", err)
		return
	}
	defer up.Close()

	pctx := ctx
	var upToxic, downToxic network.Toxic
	if a != nil {
		pctx = a.ctx
		switch a.Direction {
		case network.Upstream:
			upToxic = a.Toxic
		default:
			downToxic = a.Toxic
		}
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-pctx.Done():
		case <-ctx.Done():
		case <-done:
		}
		client.Close()
		up.Close()
	}()

	var wg sync.WaitGroup
	pipe := func(dst, src net.Conn, tox network.Toxic) {
		defer wg.Done()
		var err error
		if tox != nil {
			err = tox.Pipe(pctx, src, dst)
		} else {
			_, err = io.Copy(dst, src)
		}
		var rstErr *network.RSTError
		if errors.As(err, &rstErr) {
			for _, c := range []net.Conn{client, up} {
				if tc, ok := c.(*net.TCPConn); ok {
					tc.SetLinger(0)
				}
			}
			client.Close()
			up.Close()
			return
		}
		if tc, ok := dst.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}
	wg.Add(2)
	go pipe(up, client, upToxic)
	go pipe(client, up, downToxic)
	wg.Wait()
	close(done)
}
