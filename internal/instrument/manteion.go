package instrument

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/wire"
)

// connectManteion connects this SDK to the manteion control plane.
//
// Blocks until manteion is ready, registers the instance, fetches initial
// rules, configures the evaluator, and starts a background poll loop.
//
// Returns (nil, nil) if cfg.url is empty (offline mode — tracing-only). All
// methods on *ManteionClient are nil-receiver safe, so callers can
// unconditionally defer client.Close(ctx) without branching.
//
// Returns a non-nil error if:
//   - cfg.targets.Evaluator is nil
//   - manteion is unreachable past cfg.initTimeout
//   - registration fails
func connectManteion(ctx context.Context, cfg manteionConfig) (*ManteionClient, error) {
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.offline || cfg.url == "" {
		cfg.logger.Warn("manteion: running in offline mode (MANTEION_URL is empty or offline mode enabled)")
		return nil, nil
	}

	if cfg.serviceVersion == "" {
		cfg.logger.Warn("manteion: serviceVersion is empty; experiments may not attribute correctly",
			"hint", "set MANTEION_SERVICE_VERSION or Config.Version")
	}

	if cfg.targets.Evaluator == nil {
		return nil, errors.New("connectManteion: targets.Evaluator is required")
	}
	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.pollInterval <= 0 {
		cfg.pollInterval = 10 * time.Second
	}

	// SSE client: clone the user's transport (or DefaultTransport) so the
	// SSE connection has its own pool and doesn't compete with short-lived
	// calls. MaxIdleConnsPerHost=1 because we only hold one SSE connection
	// per client; IdleConnTimeout=0 because the stream holds itself open.
	base := http.DefaultTransport.(*http.Transport)
	if t, ok := cfg.httpClient.Transport.(*http.Transport); ok && t != nil {
		base = t
	}
	sseTransport := base.Clone()
	sseTransport.MaxIdleConnsPerHost = 1
	sseTransport.IdleConnTimeout = 0
	sseClient := &http.Client{Transport: sseTransport}

	c := &ManteionClient{
		cfg:        cfg,
		httpClient: cfg.httpClient,
		sseClient:  sseClient,
		targets:    cfg.targets,
		logger:     cfg.logger,
	}
	c.status.Store(int32(manteionDisconnected))

	if err := c.waitForReady(ctx); err != nil {
		return nil, err
	}

	if err := c.register(ctx); err != nil {
		return nil, fmt.Errorf("connectManteion: register: %w", err)
	}

	if err := c.fetchRules(ctx); err != nil {
		// Non-fatal: log and continue — poll loop will retry.
		c.logger.Warn("initial rule fetch failed, starting with empty rules", "error", err)
	} else {
		c.lastPollAt.Store(time.Now().UnixNano())
	}

	pollCtx, cancel := context.WithCancel(context.Background())
	c.pollCtx = pollCtx
	c.cancel = cancel

	// Channel used by the SSE listener to trigger an immediate poll tick.
	triggerPoll := make(chan struct{}, 1)

	c.wg.Go(func() { c.pollLoopWithTrigger(pollCtx, triggerPoll) })
	c.wg.Go(func() {
		c.listenSSE(pollCtx, func() {
			select {
			case triggerPoll <- struct{}{}:
			default:
			}
		})
	})
	if c.targets.DemoEval != nil {
		c.wg.Go(func() {
			startFaultWatchdog(pollCtx, c.targets.DemoEval, c.targets.Registry, cfg.pollInterval, c.logger)
		})
	}
	c.status.Store(int32(manteionConnected))

	return c, nil
}

// manteionConfig is the connectManteion input. defaultManteionConfig derives
// the env-driven defaults; Serve overlays the Config-supplied fields.
type manteionConfig struct {
	serviceName    string
	serviceVersion string
	address        string
	url            string
	instanceID     string
	routes         []wire.Route // published in the register payload
	host           string       // advertised instrument host (register payload)
	capabilities   []string     // advertised fault categories (register payload)
	initTimeout    time.Duration
	pollInterval   time.Duration
	offline        bool
	targets        applyTargets
	httpClient     *http.Client
	logger         *slog.Logger
	authFn         func(*http.Request) error
}

// defaultManteionConfig resolves the env-derived defaults:
// MANTEION_URL, MANTEION_INSTANCE_ID (falling back to hostname, then the
// service name), MANTEION_INIT_TIMEOUT (default 30s),
// MANTEION_ADVERTISE_ADDR (falling back to the first non-loopback IPv4),
// and MANTEION_SERVICE_VERSION.
func defaultManteionConfig(serviceName string) manteionConfig {
	url := os.Getenv("MANTEION_URL")

	instanceID := os.Getenv("MANTEION_INSTANCE_ID")
	if instanceID == "" {
		if h, err := os.Hostname(); err == nil && h != "" {
			instanceID = h
		} else {
			instanceID = serviceName
		}
	}

	initTimeout := 30 * time.Second
	if v := os.Getenv("MANTEION_INIT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			initTimeout = d
		}
	}

	addr := os.Getenv("MANTEION_ADVERTISE_ADDR")
	if addr == "" {
		addr = localIPv4()
	}

	return manteionConfig{
		serviceName:    serviceName,
		serviceVersion: os.Getenv("MANTEION_SERVICE_VERSION"),
		address:        addr,
		url:            url,
		instanceID:     instanceID,
		initTimeout:    initTimeout,
		pollInterval:   10 * time.Second,
		httpClient:     &http.Client{Timeout: 10 * time.Second},
		logger:         slog.Default(),
	}
}

// localIPv4 returns the first non-loopback IPv4 address found on any local
// network interface, or "" if none. Used as a platform-agnostic fallback for
// the manteion register payload's Address field when MANTEION_ADVERTISE_ADDR
// is not set.
//
// Caveat: on multi-NIC hosts (or with virtual bridges like docker0/cni0),
// this picks the first matching interface in iteration order, which may not
// be the address manteion can actually reach. Prefer explicit configuration
// in production.
func localIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, a := range addrs {
		ipnet, ok := a.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipnet.IP.To4(); ip4 != nil {
			return ip4.String()
		}
	}
	return ""
}

// ConnectConfig is the host-facing input to ConnectManteion. Env-derived
// defaults (MANTEION_ADVERTISE_ADDR, MANTEION_INIT_TIMEOUT, the 10s poll
// interval) come from defaultManteionConfig; the fields here overlay them.
type ConnectConfig struct {
	// Core is the instrument the control plane drives. Its identity
	// (service, instance) is what registers.
	Core *Core
	// URL is the control-plane base URL; empty = offline (returns nil, nil).
	URL            string
	ServiceVersion string
	// Routes is the HTTP route inventory published at registration.
	Routes []wire.Route
	// Address overrides the advertised address manteion fans out to
	// (default: MANTEION_ADVERTISE_ADDR, else the first non-loopback IPv4
	// with no port). Out-of-process hosts pass their bound control address.
	Address string
	// NetworkResolver maps network-fault targets to (listen, upstream)
	// pairs; with nil, network host=proxy rules are skipped per rule.
	NetworkResolver wire.NetworkResolver
	Logger          *slog.Logger
}

// ConnectManteion connects a host's core to the control plane: waits for
// readiness, registers, fetches initial rules, and starts the poll/SSE/
// watchdog goroutines. Returns (nil, nil) when URL is empty (offline).
func ConnectManteion(ctx context.Context, c ConnectConfig) (*ManteionClient, error) {
	cfg := defaultManteionConfig(c.Core.service)
	cfg.url = c.URL
	cfg.instanceID = c.Core.instance
	cfg.serviceVersion = c.ServiceVersion
	cfg.routes = c.Routes
	cfg.host = c.Core.hostName
	cfg.capabilities = c.Core.capabilities
	if c.Address != "" {
		cfg.address = c.Address
	}
	cfg.targets = c.Core.targets(c.NetworkResolver)
	if c.Logger != nil {
		cfg.logger = c.Logger
	}
	return connectManteion(ctx, cfg)
}

// DefaultAdvertiseAddr is the address an out-of-process host registers
// under: MANTEION_ADVERTISE_ADDR verbatim, else the first non-loopback IPv4
// joined with the port of controlAddr (a bound "host:port"), so manteion's
// fanout reaches the control surface without the pod manifest spelling
// the port out (design §5.3; D1(b)).
func DefaultAdvertiseAddr(controlAddr string) string {
	if v := os.Getenv("MANTEION_ADVERTISE_ADDR"); v != "" {
		return v
	}
	ip := localIPv4()
	_, port, err := net.SplitHostPort(controlAddr)
	if err != nil || port == "" || ip == "" {
		return ip
	}
	return net.JoinHostPort(ip, port)
}
