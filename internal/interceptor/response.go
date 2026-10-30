package interceptor

import "net/http"

// StatusRecorder is the ResponseWriter wrapper the ingress layers use to
// learn the status a handler wrote: the metrics layer labels its series with
// it and the fault middleware feeds it to the Transient point. The status is
// 200 until the first WriteHeader or Write (what net/http sends for a
// handler that writes nothing). Flush is forwarded and Unwrap exposes the
// underlying writer so http.ResponseController reaches Hijack and friends.
type StatusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

// NewStatusRecorder wraps w.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w, status: http.StatusOK}
}

// Status is the recorded status code.
func (r *StatusRecorder) Status() int { return r.status }

// WriteHeader records code on its first call and forwards every call.
func (r *StatusRecorder) WriteHeader(code int) {
	if !r.written {
		r.status = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Write marks the response started (an implicit 200) and forwards b.
func (r *StatusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.status = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

// Flush forwards to the underlying writer when it can flush.
func (r *StatusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap returns the underlying writer for http.ResponseController.
func (r *StatusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}
