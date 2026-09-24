package proxy

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/fault/network"
)

// HijackMode says how the egress handler must take over the app-facing
// connection to reproduce a connection-level toxic.
type HijackMode int

const (
	// HijackRST forwards Resp until AfterBytes/AfterDuration fire, then
	// resets the connection (SetLinger(0) + Close).
	HijackRST HijackMode = iota + 1
	// HijackBlackhole never answers: the connection is held open until the
	// policy ends or the client gives up.
	HijackBlackhole
)

// HijackError is returned by ToxicTransport instead of a response when the
// toxic must be observed at the connection level. The egress handler's
// error path reproduces it on the client leg (design §5.6): what the app
// sees is a reset or a hang, not an HTTP error the proxy invented.
type HijackError struct {
	Mode          HijackMode
	Policy        string
	Resp          *http.Response  // RST: the upstream response to forward until the trigger
	AfterBytes    int64           // RST: bytes to forward before the reset (0 = not by bytes)
	AfterDuration time.Duration   // RST: time before the reset (0 = not by time)
	Hold          context.Context // blackhole: hold until done
}

func (e *HijackError) Error() string {
	switch e.Mode {
	case HijackRST:
		return fmt.Sprintf("toxic %s: rst after %d bytes / %s", e.Policy, e.AfterBytes, e.AfterDuration)
	case HijackBlackhole:
		return fmt.Sprintf("toxic %s: blackhole", e.Policy)
	}
	return fmt.Sprintf("toxic %s: hijack mode %d", e.Policy, e.Mode)
}

// ToxicTransport applies armed inline network toxics to live upstream calls.
// It sits under the interceptor's egress transport: replayed (frozen)
// requests never reach it, exactly as they never reach the network.
//
// Stream toxics (latency, throttle, drip, retransmit_delay) shape the
// request body (upstream direction) or the response body (downstream
// direction) through the toxic's Pipe. Connection toxics (rst, blackhole)
// cannot be expressed as a response, so they surface as *HijackError for
// the egress handler to play on the client connection.
type ToxicTransport struct {
	Base  http.RoundTripper // nil → http.DefaultTransport
	Table *PolicyTable
}

func (t *ToxicTransport) base() http.RoundTripper {
	if t.Base != nil {
		return t.Base
	}
	return http.DefaultTransport
}

// RoundTrip implements http.RoundTripper.
func (t *ToxicTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.Table == nil {
		return t.base().RoundTrip(r)
	}
	a, ok := t.Table.match(r.URL.Host)
	if !ok {
		return t.base().RoundTrip(r)
	}

	switch tox := a.Toxic.(type) {
	case *network.Blackhole:
		// The request never reaches the upstream, as with the TCP proxy's
		// pre-dial hijack.
		return nil, &HijackError{Mode: HijackBlackhole, Policy: a.Name, Hold: a.ctx}
	case *network.RST:
		resp, err := t.base().RoundTrip(r)
		if err != nil {
			return nil, err
		}
		return nil, &HijackError{Mode: HijackRST, Policy: a.Name, Resp: resp,
			AfterBytes: tox.AfterBytes, AfterDuration: tox.AfterDuration}
	}

	if a.Direction == network.Upstream {
		if r.Body != nil && r.Body != http.NoBody {
			r = r.Clone(r.Context())
			r.Body = pipeThrough(a.ctx, r.Body, a.Toxic)
			r.GetBody = nil
		}
		return t.base().RoundTrip(r)
	}

	resp, err := t.base().RoundTrip(r)
	if err != nil {
		return resp, err
	}
	resp.Body = pipeThrough(a.ctx, resp.Body, a.Toxic)
	// Unknown length: the reverse proxy then flushes every write, so the
	// toxic's timing reaches the client as it happens instead of being
	// absorbed by buffering. The framing changes to chunked; the bytes
	// and their timing are what the experiment observes.
	resp.ContentLength = -1
	resp.Header.Del("Content-Length")
	return resp, nil
}

// pipeThrough runs toxic.Pipe(ctx, src, w) on its own goroutine and returns
// the read end. The toxic's clean completion (nil) closes the pipe with EOF;
// its error (ctx expiry, RST) reaches the reader.
func pipeThrough(ctx context.Context, src io.ReadCloser, toxic network.Toxic) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		err := toxic.Pipe(ctx, src, pw)
		src.Close()
		pw.CloseWithError(err)
	}()
	return pr
}
