package instrument

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlSurface_MountsEveryPathManteionAddresses(t *testing.T) {
	c := NewCore(CoreConfig{Service: "svc", Instance: "pod-1"})
	t.Cleanup(c.Stop)
	ts := httptest.NewServer(ControlSurface(ControlConfig{Core: c}))
	t.Cleanup(ts.Close)

	cases := []struct {
		method, path, body string
		want               int
	}{
		{"GET", "/metrics", "", 200},
		{"GET", "/atropos/health", "", 200},
		{"GET", "/admin/fault", "", 200},
		{"DELETE", "/admin/fault/x", "", 200},
		{"GET", "/admin/rules", "", 200},
		{"POST", "/admin/rules", "[]", 204},
		{"GET", "/admin/cachebox", "", 200},
		{"POST", "/admin/cachebox/delay", `{"mu":1,"sigma":0.1}`, 204},
		{"POST", "/cachebox/preload/abort", `{"experiment_id":"e","phase_id":"p"}`, 200},
		{"GET", "/cachebox/fidelity?experiment_id=e&phase_id=p", "", 200},
		{"GET", "/anything-else", "", 404},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(tc.body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", tc.method, tc.path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s = %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
}
