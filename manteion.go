package atropos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

// ConnectManteion connects this SDK to the manteion control plane.
//
// Blocks until manteion is ready, registers the instance, fetches initial
// rules, configures the evaluator, and starts a background poll loop.
//
// URL resolution: opts > MANTEION_URL env > offline mode.
//
// Returns (nil, nil) if no URL is set (offline mode — tracing-only). All
// public methods on *ManteionClient are nil-receiver safe, so callers can
// unconditionally defer client.Close(ctx) without branching.
//
// Returns a non-nil error if:
//   - WithApplyTargets is not set or ApplyTargets.Evaluator is nil
//   - manteion is unreachable past InitTimeout
//   - registration or initial rule fetch fails
func ConnectManteion(ctx context.Context, serviceName string, opts ...ManteionOption) (*ManteionClient, error) {
	cfg := defaultManteionConfig(serviceName)
	for _, o := range opts {
		o.applyManteion(&cfg)
	}

	if cfg.offline || cfg.url == "" {
		return nil, nil
	}

	if cfg.serviceVersion == "" {
		cfg.logger.Warn("manteion: serviceVersion is empty; experiments may not attribute correctly",
			"hint", "set MANTEION_SERVICE_VERSION or pass WithManteionServiceVersion()")
	}

	if cfg.targets.Evaluator == nil {
		return nil, errors.New("ConnectManteion: WithApplyTargets must be set with a non-nil Evaluator")
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
	c.status.Store(int32(ManteionDisconnected))

	if err := c.waitForReady(ctx); err != nil {
		return nil, err
	}

	if err := c.register(ctx); err != nil {
		return nil, fmt.Errorf("ConnectManteion: register: %w", err)
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
	c.status.Store(int32(ManteionConnected))
	setGlobalClient(c)

	return c, nil
}

// ManteionOption is a functional option for ConnectManteion.
type ManteionOption interface{ applyManteion(*manteionConfig) }

type manteionOptionFunc func(*manteionConfig)

func (f manteionOptionFunc) applyManteion(c *manteionConfig) { f(c) }

// WithManteionURL sets the base URL of the manteion control plane.
func WithManteionURL(url string) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.url = url })
}

// WithInstanceID overrides the auto-generated instance ID.
func WithInstanceID(id string) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.instanceID = id })
}

// WithInitTimeout sets the maximum time to wait for manteion readiness.
// Default: 30s.
func WithInitTimeout(d time.Duration) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.initTimeout = d })
}

// WithPollInterval sets the rule poll cadence. Default: 10s.
func WithPollInterval(d time.Duration) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.pollInterval = d })
}

// WithOfflineMode forces offline (tracing-only) mode even if MANTEION_URL is set.
func WithOfflineMode() ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.offline = true })
}

// WithApplyTargets sets the SDK objects that ConnectManteion will configure.
// Evaluator is required.
func WithApplyTargets(t ApplyTargets) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.targets = t })
}

// WithAddress sets the advertised address included in the register payload.
// Precedence: WithAddress > MANTEION_ADVERTISE_ADDR > POD_IP > "".
func WithAddress(addr string) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.address = addr })
}

// WithManteionServiceVersion sets the service version reported in the register
// payload. Experiments use this to attribute fault behaviour to a specific
// version. Precedence: WithManteionServiceVersion > MANTEION_SERVICE_VERSION > "".
func WithManteionServiceVersion(v string) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.serviceVersion = v })
}

// WithHTTPClient injects a custom *http.Client. Default: 10s timeout.
func WithHTTPClient(hc *http.Client) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.httpClient = hc })
}

// WithAuthFunc applies fn to every outgoing manteion request. Use for
// rotation, JWT refresh, signed requests, etc. Called per-request; keep cheap.
func WithAuthFunc(fn func(*http.Request) error) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.authFn = fn })
}

// WithAuthHeader is a convenience for static header injection.
// Equivalent to WithAuthFunc that sets req.Header.Set(name, value).
func WithAuthHeader(name, value string) ManteionOption {
	return WithAuthFunc(func(r *http.Request) error {
		r.Header.Set(name, value)
		return nil
	})
}

// WithLogger sets the slog logger used by ManteionClient. Default: slog.Default().
func WithLogger(l *slog.Logger) ManteionOption {
	return manteionOptionFunc(func(c *manteionConfig) { c.logger = l })
}

type manteionConfig struct {
	serviceName    string
	serviceVersion string
	address        string
	url            string
	instanceID     string
	initTimeout    time.Duration
	pollInterval   time.Duration
	offline        bool
	targets        ApplyTargets
	httpClient     *http.Client
	logger         *slog.Logger
	authFn         func(*http.Request) error
}

func defaultManteionConfig(serviceName string) manteionConfig {
	url := os.Getenv("MANTEION_URL")

	instanceID := os.Getenv("MANTEION_INSTANCE_ID")
	if instanceID == "" {
		instanceID = generateInstanceID()
	}

	initTimeout := 30 * time.Second
	if v := os.Getenv("MANTEION_INIT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			initTimeout = d
		}
	}

	addr := os.Getenv("MANTEION_ADVERTISE_ADDR")
	if addr == "" {
		addr = os.Getenv("POD_IP")
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
