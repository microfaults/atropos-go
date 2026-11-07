package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
	"git.ucsc.edu/microfaults/atropos-go/internal/fault"
	"git.ucsc.edu/microfaults/atropos-go/internal/instrument"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// AgentFault is the sidecar-side fault.Fault for a resource rule: Start
// opens the agent's streaming request, the fault runs in the target cgroup
// until it ends or the handle is stopped, and the agent's result lands on
// the sidecar's fault span like any other Result.
type AgentFault struct {
	fault.FaultConfig
	Sock      string
	PodUID    string
	Container string
	Name      string
	Req       wire.FaultRequest
}

// Validate implements fault.Fault.
func (f *AgentFault) Validate() error {
	if err := f.FaultConfig.Validate(); err != nil {
		return err
	}
	if f.Sock == "" {
		return errors.New("agent: fault requires the agent socket path")
	}
	if f.Container == "" {
		return errors.New("agent: fault requires the target container name")
	}
	return nil
}

func socketClient(sock string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", sock)
		},
	}}
}

// Start implements fault.Fault.
func (f *AgentFault) Start(ctx context.Context) (*fault.Handle, error) {
	if err := f.Validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	handle := fault.NewHandle(cancel)
	client := socketClient(f.Sock)

	body, _ := json.Marshal(StartRequest{PodUID: f.PodUID, Container: f.Container, Name: f.Name, Fault: f.Req})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://atropos-agent/v1/faults", bytes.NewReader(body))
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("agent: start: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("agent: start rejected (%d): %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	if !sc.Scan() {
		resp.Body.Close()
		cancel()
		return nil, errors.New("agent: start: no id in response")
	}
	var sr StartResponse
	if err := json.Unmarshal(sc.Bytes(), &sr); err != nil || sr.ID == "" {
		resp.Body.Close()
		cancel()
		return nil, errors.New("agent: start: malformed id line")
	}

	start := time.Now()
	go func() {
		defer resp.Body.Close()
		for sc.Scan() {
			var r Result
			if json.Unmarshal(sc.Bytes(), &r) != nil {
				continue
			}
			res := fault.Result{ActualDuration: time.Duration(r.ActualDurationMs) * time.Millisecond, Detail: r.Detail}
			if r.Error != "" {
				res.Err = errors.New(r.Error)
			}
			handle.Send(res)
			return
		}
		err := sc.Err()
		if err == nil {
			err = errors.New("agent: stream ended without a result")
		}
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		handle.Send(fault.Result{ActualDuration: time.Since(start), Err: err})
	}()
	go func() {
		// Stop: tell the agent explicitly (the stream close would do it too,
		// but the DELETE makes the stop immediate and logged).
		<-ctx.Done()
		sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer scancel()
		dreq, _ := http.NewRequestWithContext(sctx, http.MethodDelete, "http://atropos-agent/v1/faults/"+sr.ID, nil)
		if dresp, err := client.Do(dreq); err == nil {
			dresp.Body.Close()
		}
	}()
	return handle, nil
}

// ResourceFactory is the instrument.ResourceHost a sidecar installs: every
// resource rule becomes an AgentFault for this pod's app container.
func ResourceFactory(sock, podUID, container string) instrument.ResourceHost {
	return func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		return &AgentFault{FaultConfig: base, Sock: sock, PodUID: podUID, Container: container, Name: name, Req: *req}, nil
	}
}

// Reachable reports whether an agent answers on sock (GET /v1/faults).
func Reachable(sock string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://atropos-agent/v1/faults", nil)
	resp, err := socketClient(sock).Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// NetworkFactory is the instrument.NetworkHost a sidecar installs when the
// agent offers netem: every netem network rule becomes an AgentFault for this
// pod's app container. It validates the request with NetemSpecFromRequest
// first, the same check the agent runs at start, so a malformed netem rule
// fails at decode with the agent's own message: a decode error (a bad rule,
// like an unparseable delay), never an UnsupportedFaultError.
func NetworkFactory(sock, podUID, container string) instrument.NetworkHost {
	return func(name string, req *wire.FaultRequest, base fault.FaultConfig) (fault.Fault, error) {
		if _, err := NetemSpecFromRequest(*req); err != nil {
			return nil, err
		}
		return &AgentFault{FaultConfig: base, Sock: sock, PodUID: podUID, Container: container, Name: name, Req: *req}, nil
	}
}

// Capabilities asks the agent on sock which mechanisms it executes (GET
// /v1/capabilities, 2 s timeout). A 200 is the decoded body. A 404 is an
// agent predating the endpoint, which executes burn and quota only: it
// answers {burn, quota} and a nil error. A dial failure or any other status
// is an error (no live agent: the sidecar delegates nothing to it).
func Capabilities(sock string) (CapabilitiesResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://atropos-agent/v1/capabilities", nil)
	if err != nil {
		return CapabilitiesResponse{}, err
	}
	resp, err := socketClient(sock).Do(req)
	if err != nil {
		return CapabilitiesResponse{}, fmt.Errorf("agent: capabilities: %w", err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var ac CapabilitiesResponse
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&ac); err != nil {
			return CapabilitiesResponse{}, fmt.Errorf("agent: capabilities: malformed body: %w", err)
		}
		return ac, nil
	case http.StatusNotFound:
		return CapabilitiesResponse{Mechanisms: []string{faultparams.MechanismBurn, faultparams.MechanismQuota}}, nil
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return CapabilitiesResponse{}, fmt.Errorf("agent: capabilities (%d): %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
}

var _ fault.Fault = (*AgentFault)(nil)
