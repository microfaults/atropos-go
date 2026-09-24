// Package atroposd is the out-of-process host of the atropos instrument: one
// sidecar per pod that builds the same core the in-process atropos.Serve
// embed builds, connects it to manteion as one SDK instance, and puts the
// proxy listeners of internal/proxy around the app. Design:
// docs/plans/2026-09-08-atropos-daemon-design.md.
package atroposd

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"git.ucsc.edu/microfaults/atropos-go/internal/agent"
	"git.ucsc.edu/microfaults/atropos-go/internal/proxy"
	"git.ucsc.edu/microfaults/atropos-go/internal/wire"

	oteltrace "go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"
)

// Capture modes.
const (
	// CaptureExplicit: the app dials the sidecar's egress ports (one per
	// dependency); the sidecar rewrites Host to the real upstream.
	CaptureExplicit = "explicit"
	// CaptureTransparent: iptables redirects the app's connections to the
	// sidecar, which recovers the original destination (M2).
	CaptureTransparent = "transparent"
)

// Defaults.
const (
	DefaultControlListen   = ":16020"
	DefaultInboundUpstream = "127.0.0.1:8080"
	DefaultEgressListen    = ":16001"
	DefaultIngressListen   = ":16006"
	DefaultAppHost         = "127.0.0.1"
	DefaultCaptureMarker   = "/var/run/atropos/capture.ok"
	DefaultAppContainer    = "server"
)

// EgressRoute is one explicit-mode egress listener.
type EgressRoute struct {
	Listen   string `yaml:"listen"`   // e.g. ":19090"
	Upstream string `yaml:"upstream"` // e.g. "productcatalogservice:3550"
}

// RouteConfig is one HTTP route published at registration (wire.Route with
// yaml keys).
type RouteConfig struct {
	Method      string   `yaml:"method"`
	Path        string   `yaml:"path"`
	Description string   `yaml:"description"`
	DependsOn   []string `yaml:"depends_on"`
}

// Config configures Run. Every field has a yaml key and, where a pod
// manifest would set it, an environment variable (see Load).
type Config struct {
	Service         string        `yaml:"service"`
	Version         string        `yaml:"version"`
	Environment     string        `yaml:"environment"`
	ManteionURL     string        `yaml:"manteion_url"`
	AdvertiseAddr   string        `yaml:"advertise_addr"`
	InstanceID      string        `yaml:"instance_id"`
	ControlListen   string        `yaml:"control_listen"`
	IngressListen   string        `yaml:"ingress_listen"` // "" disables the ingress listener
	InboundUpstream string        `yaml:"inbound_upstream"`
	Capture         string        `yaml:"capture"`
	Egress          []EgressRoute `yaml:"egress"` // explicit mode
	Routes          []RouteConfig `yaml:"routes"`

	// Transparent mode (design §5.2): the init container REDIRECTs the
	// app's outbound TCP to EgressListen and InboundPorts to IngressListen.
	EgressListen  string `yaml:"egress_listen"`  // default ":16001"
	InboundPorts  []int  `yaml:"inbound_ports"`  // app ports captured at ingress; empty = none
	RawTCPPorts   []int  `yaml:"raw_tcp_ports"`  // non-HTTP dependencies relayed at L4
	AppHost       string `yaml:"app_host"`       // where the app listens; default 127.0.0.1
	CaptureMarker string `yaml:"capture_marker"` // written by the init container; default /var/run/atropos/capture.ok

	// Node agent (design §5.14): when the socket answers at startup, the
	// sidecar advertises the resource capability and delegates resource
	// faults to the agent for AppContainer of this pod.
	AgentSocket  string `yaml:"agent_socket"`  // default /var/run/atropos/agent.sock
	AppContainer string `yaml:"app_container"` // default "server"
	PodUID       string `yaml:"pod_uid"`       // downward API metadata.uid (ATROPOS_POD_UID)

	// TracerProvider, if set, replaces the OTLP exporter (tests, or hosts
	// that own their OTel setup).
	TracerProvider oteltrace.TracerProvider `yaml:"-"`
	// OriginalDst overrides the original-destination lookup (tests). When
	// set, the capture marker is not required.
	OriginalDst proxy.OriginalDstFunc `yaml:"-"`
	// OnReady is called once every listener is bound and the control plane
	// is connected, with the bound addresses.
	OnReady func(Addrs) `yaml:"-"`
}

// Load reads the optional YAML file at path, overlays the environment, and
// applies defaults. The environment wins over the file: it is what the pod
// manifest sets.
//
// Environment: ATROPOS_SERVICE, MANTEION_SERVICE_VERSION, MANTEION_URL,
// MANTEION_ADVERTISE_ADDR, MANTEION_INSTANCE_ID, ATROPOSD_CONTROL_ADDR,
// ATROPOSD_INGRESS_ADDR, ATROPOSD_INBOUND_UPSTREAM, ATROPOSD_CAPTURE, and
// ATROPOSD_EGRESS as comma-separated "listen=upstream" pairs
// (":19090=productcatalogservice:3550,:19091=currencyservice:7000").
func Load(path string) (Config, error) {
	var cfg Config
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("atroposd: read config: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, fmt.Errorf("atroposd: parse config %s: %w", path, err)
		}
	}
	cfg.overlayEnv()
	cfg.applyDefaults()
	return cfg, nil
}

func (c *Config) overlayEnv() {
	set := func(dst *string, key string) {
		if v := os.Getenv(key); v != "" {
			*dst = v
		}
	}
	set(&c.Service, "ATROPOS_SERVICE")
	set(&c.Version, "MANTEION_SERVICE_VERSION")
	set(&c.ManteionURL, "MANTEION_URL")
	set(&c.AdvertiseAddr, "MANTEION_ADVERTISE_ADDR")
	set(&c.InstanceID, "MANTEION_INSTANCE_ID")
	set(&c.ControlListen, "ATROPOSD_CONTROL_ADDR")
	set(&c.IngressListen, "ATROPOSD_INGRESS_ADDR")
	set(&c.InboundUpstream, "ATROPOSD_INBOUND_UPSTREAM")
	set(&c.Capture, "ATROPOSD_CAPTURE")
	set(&c.EgressListen, "ATROPOSD_EGRESS_ADDR")
	set(&c.AgentSocket, "ATROPOSD_AGENT_SOCKET")
	set(&c.AppContainer, "ATROPOSD_APP_CONTAINER")
	set(&c.PodUID, "ATROPOS_POD_UID")
	set(&c.AppHost, "ATROPOSD_APP_HOST")
	set(&c.CaptureMarker, "ATROPOSD_CAPTURE_MARKER")
	if v := os.Getenv("ATROPOSD_INBOUND_PORTS"); v != "" {
		c.InboundPorts = parsePorts(v)
	}
	if v := os.Getenv("ATROPOSD_RAW_TCP_PORTS"); v != "" {
		c.RawTCPPorts = parsePorts(v)
	}
	if v := os.Getenv("ATROPOSD_EGRESS"); v != "" {
		c.Egress = nil
		for _, item := range strings.Split(v, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			listen, upstream, _ := strings.Cut(item, "=")
			c.Egress = append(c.Egress, EgressRoute{Listen: listen, Upstream: upstream})
		}
	}
}

// parsePorts parses a comma-separated port list; junk entries become -1 so
// Validate reports them.
func parsePorts(v string) []int {
	var out []int
	for _, item := range strings.Split(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		p, err := strconv.Atoi(item)
		if err != nil {
			p = -1
		}
		out = append(out, p)
	}
	return out
}

func (c *Config) applyDefaults() {
	if c.ControlListen == "" {
		c.ControlListen = DefaultControlListen
	}
	if c.InboundUpstream == "" {
		c.InboundUpstream = DefaultInboundUpstream
	}
	if c.Capture == "" {
		c.Capture = CaptureExplicit
	}
	if c.AgentSocket == "" {
		c.AgentSocket = agent.DefaultSocket
	}
	if c.AppContainer == "" {
		c.AppContainer = DefaultAppContainer
	}
	if c.Capture == CaptureTransparent {
		if c.EgressListen == "" {
			c.EgressListen = DefaultEgressListen
		}
		if len(c.InboundPorts) > 0 && c.IngressListen == "" {
			c.IngressListen = DefaultIngressListen
		}
		if c.AppHost == "" {
			c.AppHost = DefaultAppHost
		}
		if c.CaptureMarker == "" {
			c.CaptureMarker = DefaultCaptureMarker
		}
	}
}

// Validate reports every configuration error at once.
func (c Config) Validate() error {
	var errs []error
	if c.Service == "" {
		errs = append(errs, errors.New("service is required (ATROPOS_SERVICE)"))
	}
	switch c.Capture {
	case CaptureExplicit:
		if len(c.Egress) == 0 && c.IngressListen == "" {
			errs = append(errs, errors.New("explicit capture needs at least one egress listener (egress: / ATROPOSD_EGRESS) or an ingress listener"))
		}
		for i, e := range c.Egress {
			if e.Listen == "" || e.Upstream == "" {
				errs = append(errs, fmt.Errorf("egress[%d]: listen and upstream are both required (listen=upstream)", i))
			}
		}
	case CaptureTransparent:
		if c.EgressListen == "" {
			errs = append(errs, errors.New("transparent capture needs egress_listen"))
		}
		for _, p := range append(append([]int(nil), c.InboundPorts...), c.RawTCPPorts...) {
			if p <= 0 || p > 65535 {
				errs = append(errs, fmt.Errorf("port %d is not a valid TCP port", p))
			}
		}
	default:
		errs = append(errs, fmt.Errorf("unknown capture mode %q (explicit|transparent)", c.Capture))
	}
	if c.IngressListen != "" && c.InboundUpstream == "" {
		errs = append(errs, errors.New("ingress_listen requires inbound_upstream"))
	}
	return errors.Join(errs...)
}

func (c Config) wireRoutes() []wire.Route {
	if len(c.Routes) == 0 {
		return nil
	}
	out := make([]wire.Route, len(c.Routes))
	for i, r := range c.Routes {
		out[i] = wire.Route{Method: r.Method, Path: r.Path, Description: r.Description, DependsOn: r.DependsOn}
	}
	return out
}
