package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/faultparams"
)

// Server executes resource faults for sidecars over a Unix socket.
type Server struct {
	Resolver TargetResolver
	Journal  Journal
	// Self is the agent binary re-executed as the stress child.
	Self string
	// Peer identifies the caller's pod; nil refuses every request (there
	// is no safe default). Tests inject one.
	Peer   PeerPodUIDFunc
	Host   Capacity
	Logger *slog.Logger

	mu      sync.Mutex
	running map[string]*runningFault
}

type runningFault struct {
	st   Status
	stop func()
	done <-chan Result
}

type peerInfo struct {
	uid string
	err error
}

type peerKey struct{}

// ConnContext is the http.Server hook that stamps the caller's pod on every
// request of a connection.
func (s *Server) ConnContext(ctx context.Context, c net.Conn) context.Context {
	if s.Peer == nil {
		return context.WithValue(ctx, peerKey{}, peerInfo{err: errors.New("agent: no peer identification configured")})
	}
	uid, err := s.Peer(c)
	return context.WithValue(ctx, peerKey{}, peerInfo{uid: uid, err: err})
}

func (s *Server) logger() *slog.Logger {
	if s.Logger != nil {
		return s.Logger
	}
	return slog.Default()
}

// Handler serves the agent API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/faults", s.start)
	mux.HandleFunc("DELETE /v1/faults/{id}", s.stopHandler)
	mux.HandleFunc("GET /v1/faults", s.list)
	return mux
}

// Serve runs the API on ln until ctx ends.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: s.Handler(), ConnContext: s.ConnContext, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		s.stopAll()
	}()
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Listen binds the Unix socket (replacing a stale file) with permissions
// that let the sidecars' uid connect; the peer check is the authorization.
func Listen(sock string) (net.Listener, error) {
	if err := os.MkdirAll(dirOf(sock), 0o755); err != nil {
		return nil, err
	}
	_ = os.Remove(sock)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(sock, 0o666); err != nil {
		ln.Close()
		return nil, err
	}
	return ln, nil
}

func dirOf(p string) string {
	if i := strings.LastIndex(p, "/"); i > 0 {
		return p[:i]
	}
	return "."
}

func (s *Server) stopAll() {
	s.mu.Lock()
	rs := make([]*runningFault, 0, len(s.running))
	for _, r := range s.running {
		rs = append(rs, r)
	}
	s.mu.Unlock()
	for _, r := range rs {
		r.stop()
	}
}

type resourceParams struct {
	TargetLoad float64 `json:"target_load"`
	Mechanism  string  `json:"mechanism"`
}

func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	pi, _ := r.Context().Value(peerKey{}).(peerInfo)
	if pi.err != nil {
		http.Error(w, "cannot identify the calling pod: "+pi.err.Error(), http.StatusForbidden)
		return
	}
	if pi.uid != req.PodUID {
		http.Error(w, fmt.Sprintf("a sidecar may only fault its own pod (caller %s, requested %s)", pi.uid, req.PodUID), http.StatusForbidden)
		return
	}
	if req.Fault.EffectiveCategory() != "resource" {
		http.Error(w, "the agent executes resource faults only", http.StatusBadRequest)
		return
	}
	if req.Fault.DurationMs <= 0 {
		http.Error(w, "duration_ms must be > 0", http.StatusBadRequest)
		return
	}
	var p resourceParams
	_ = json.Unmarshal(req.Fault.Params, &p)
	if err := faultparams.ValidateMechanism(req.Fault.FaultType, p.Mechanism); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	t, err := s.Resolver.Resolve(req.PodUID, req.Container)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	id := newID()
	duration := time.Duration(req.Fault.DurationMs) * time.Millisecond
	var stop func()
	var done <-chan Result
	mech := p.Mechanism
	if mech == "" {
		mech = faultparams.MechanismBurn
	}
	switch mech {
	case faultparams.MechanismQuota:
		var kind QuotaKind
		switch req.Fault.FaultType {
		case "cpu":
			kind = QuotaCPU
		case "memory":
			kind = QuotaMemory
		default:
			http.Error(w, "quota is supported for cpu and memory only", http.StatusBadRequest)
			return
		}
		q, err := ApplyQuota(s.Journal, id, t.CgroupDir, kind, p.TargetLoad, s.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ch := make(chan Result, 1)
		start := time.Now()
		var once sync.Once
		finish := func(reason string) {
			once.Do(func() {
				res := Result{ActualDurationMs: time.Since(start).Milliseconds(), Detail: map[string]any{"quota": q, "ended": reason}}
				if err := RestoreQuota(s.Journal, *q); err != nil {
					res.Error = err.Error()
					s.logger().Error("agent: restore quota", "id", id, "error", err)
				}
				ch <- res
			})
		}
		timer := time.AfterFunc(duration, func() { finish("duration") })
		stop = func() {
			timer.Stop()
			finish("stopped")
		}
		done = ch
	default:
		avail := Capacity{CPUs: AvailableCPUs(t.CgroupDir, s.Host.CPUs), MemoryBytes: AvailableMemory(t.CgroupDir, s.Host.MemoryBytes)}
		b, err := StartBurn(r.Context(), s.Self, t.CgroupDir, req.Fault, avail)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		stop = b.Stop
		done = b.Done()
	}

	rf := &runningFault{stop: stop, done: done, st: Status{
		ID: id, PodUID: req.PodUID, Container: req.Container, Name: req.Name,
		Category: "resource", FaultType: req.Fault.FaultType, Mechanism: mech, CgroupDir: t.CgroupDir, StartedAt: time.Now(),
	}}
	s.mu.Lock()
	if s.running == nil {
		s.running = map[string]*runningFault{}
	}
	s.running[id] = rf
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.running, id)
		s.mu.Unlock()
	}()
	s.logger().Info("agent: fault started", "id", id, "pod", req.PodUID, "container", req.Container, "type", req.Fault.FaultType, "mechanism", mech, "cgroup", t.CgroupDir)

	// Streaming response: the id now, the result when the fault ends. The
	// fault lives as long as this request: a sidecar that stops it, or
	// dies, ends the stream and the fault is stopped/restored.
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	json.NewEncoder(w).Encode(StartResponse{ID: id})
	if flusher != nil {
		flusher.Flush()
	}
	var res Result
	select {
	case res = <-done:
	case <-r.Context().Done():
		stop()
		select {
		case res = <-done:
		case <-time.After(15 * time.Second):
			res = Result{Error: "agent: fault did not stop in time"}
		}
	}
	s.logger().Info("agent: fault ended", "id", id, "duration_ms", res.ActualDurationMs, "error", res.Error)
	json.NewEncoder(w).Encode(res)
}

func (s *Server) stopHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	s.mu.Lock()
	rf := s.running[id]
	s.mu.Unlock()
	if rf == nil {
		http.Error(w, "no such fault", http.StatusNotFound)
		return
	}
	rf.stop()
	w.WriteHeader(http.StatusOK)
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	out := make([]Status, 0, len(s.running))
	for _, rf := range s.running {
		out = append(out, rf.st)
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
