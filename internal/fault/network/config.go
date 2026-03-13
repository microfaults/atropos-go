package network

import (
	"fmt"

	fault "github.com/microfaults/atropos-go/internal/fault"
)

// Config holds parameters for TCP proxy-based network faults.
type Config struct {
	fault.FaultConfig

	// Listen is the address the proxy listens on (e.g., ":9090").
	Listen string

	// Upstream is the address to proxy traffic to (e.g., "redis:6379").
	Upstream string

	// Scope is the fraction of connections that get toxics applied [0.0, 1.0].
	// Unaffected connections pass through transparently.
	// Defaults to 1.0 if zero.
	Scope float64

	// Toxics configures which effects to apply and in which direction.
	Toxics []ToxicLink

	// External is an optional hook for delegating to an external proxy
	// (e.g., Toxiproxy) instead of the built-in TCP proxy. If set,
	// the built-in proxy is not started; the external backend is used.
	External ExternalProxy
}

// ToxicLink pairs a toxic with the direction it applies to.
type ToxicLink struct {
	Toxic     Toxic
	Direction Direction
}

// Validate checks that the network config is valid.
func (c *Config) Validate() error {
	if c.External != nil {
		// External backend handles its own validation.
		return c.FaultConfig.Validate()
	}
	if err := c.FaultConfig.Validate(); err != nil {
		return err
	}
	if c.Listen == "" {
		return fmt.Errorf("network: listen address must not be empty")
	}
	if c.Upstream == "" {
		return fmt.Errorf("network: upstream address must not be empty")
	}
	if c.Scope < 0 || c.Scope > 1.0 {
		return fmt.Errorf("network: scope must be in [0.0, 1.0], got %.2f", c.Scope)
	}
	if len(c.Toxics) == 0 {
		return fmt.Errorf("network: at least one toxic must be configured")
	}
	return nil
}

// effectiveScope returns the scope, defaulting to 1.0 if zero.
func (c *Config) effectiveScope() float64 {
	if c.Scope <= 0 {
		return 1.0
	}
	return c.Scope
}
