package atropos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"git.ucsc.edu/microfaults/atropos-go/internal/cachebox"
)

// RegisterRequest is the POST body for /api/v1/sdk/register.
type RegisterRequest struct {
	ID      string `json:"id"`
	Service string `json:"service"`
	Version string `json:"version,omitempty"`
	Address string `json:"address"`
}

// RegisterResponse is the JSON manteion returns from /api/v1/sdk/register.
// Rules, ActiveFault, and FreezeCfg are populated only when manteion has
// intent tracked for the registering service — e.g. during a rolling deploy
// while an experiment is in progress.
type RegisterResponse struct {
	Status      string         `json:"status"`
	Rules       []CompiledRule `json:"rules,omitempty"`
	ActiveFaults []FaultRequest `json:"active_faults,omitempty"`
	FreezeCfg   *DelayRequest  `json:"freeze_cfg,omitempty"`
}

// registerTimeout is the default per-call deadline for Register.
const registerTimeout = 5 * time.Second

// Register POSTs a registration to manteion using http.DefaultClient.
// The returned response may contain rules, active_fault, and freeze_cfg if
// manteion has intent tracked for the registering service.
//
// baseURL is manteion's base URL (e.g. "http://manteion.control.svc:8080").
// The request is subject to registerTimeout (5s) unless ctx has an earlier deadline.
func Register(ctx context.Context, baseURL string, req RegisterRequest) (RegisterResponse, error) {
	return registerWith(ctx, http.DefaultClient, baseURL, req)
}

// RegisterWithClient is Register but uses the supplied *http.Client.
// Use when you need explicit transport / timeout control (e.g. ManteionClient).
func RegisterWithClient(ctx context.Context, hc *http.Client, baseURL string, req RegisterRequest) (RegisterResponse, error) {
	return registerWith(ctx, hc, baseURL, req)
}

// registerWith is the private implementation shared by Register and RegisterWithClient.
// It marshals req, POSTs to baseURL+"/api/v1/sdk/register" with a 5s timeout
// (registerTimeout) layered onto ctx, and decodes the response. Response body
// is read under a 1 MiB cap; a truncated/erroring read still surfaces as a
// decode failure below, so dropping the ReadAll error loses no diagnostic value.
func registerWith(ctx context.Context, hc *http.Client, baseURL string, req RegisterRequest) (RegisterResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("marshal register request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, registerTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/v1/sdk/register", bytes.NewReader(body))
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("new register request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := hc.Do(httpReq)
	if err != nil {
		return RegisterResponse{}, fmt.Errorf("send register request: %w", err)
	}
	defer httpResp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20))
	if httpResp.StatusCode != http.StatusCreated {
		return RegisterResponse{}, fmt.Errorf("register returned status %d: %s",
			httpResp.StatusCode, string(respBody))
	}

	var resp RegisterResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return RegisterResponse{}, fmt.Errorf("decode register response: %w", err)
	}
	return resp, nil
}

// ApplyTargets names the SDK objects Apply mutates. Nil targets mean "this SDK
// instance doesn't support that capability"; if the response references that
// capability, Apply returns an error.
type ApplyTargets struct {
	// Evaluator receives the decoded rule set. Required if resp.Rules is non-empty.
	Evaluator *StaticEvaluator
	// DemoEval receives the active faults. Required if resp.ActiveFaults is non-empty.
	DemoEval *DemoEvaluator
	// CacheBox receives the freeze config. Required if resp.FreezeCfg is non-nil.
	CacheBox *CacheBox
	// NetworkResolver maps logical targets to listen/upstream pairs for network
	// fault proxies. Required if rules or active_fault contain network-category faults.
	NetworkResolver NetworkResolver
}

// Apply installs the register response's intent state onto the supplied
// targets. Each category (rules, active fault, freeze config) is independent.
// Returns an error if the response carries a category but the corresponding
// target is nil, or if any component fails to apply.
//
// A nil or empty rules slice is treated as 'no change' — only a populated
// rule list replaces the evaluator's current rules.
//
// Error messages are prefixed by category ("apply rules: ...",
// "apply active_fault: ...", "apply freeze_cfg: ...") so log grepping
// can filter a single bootstrap phase without ambiguity.
func Apply(resp RegisterResponse, targets ApplyTargets) error {
	if len(resp.Rules) > 0 {
		if targets.Evaluator == nil {
			return fmt.Errorf("apply rules: no Evaluator target for %d rules", len(resp.Rules))
		}
		var opts []DecodeOption
		if targets.NetworkResolver != nil {
			opts = append(opts, WithNetworkResolver(targets.NetworkResolver))
		}
		rules, err := DecodeCompiledRules(resp.Rules, opts...)
		if err != nil {
			return fmt.Errorf("apply rules: %w", err)
		}
		targets.Evaluator.SetRules(rules)
	}

	if len(resp.ActiveFaults) > 0 {
		if targets.DemoEval == nil {
			return fmt.Errorf("apply active_faults: no DemoEval target")
		}
	}

	if targets.DemoEval != nil {
		// Reconciliation: drop slots not in the response
		inResponse := make(map[string]bool, len(resp.ActiveFaults))
		for _, req := range resp.ActiveFaults {
			inResponse[req.effectiveCategory()] = true
		}
		for _, cat := range targets.DemoEval.ActiveCategories() {
			if !inResponse[cat] {
				targets.DemoEval.ClearSlot(cat)
			}
		}

		// Apply / refresh slots that ARE in the response
		for _, req := range resp.ActiveFaults {
			if err := applyActiveFault(req, targets.DemoEval, targets.NetworkResolver); err != nil {
				return fmt.Errorf("apply active_faults[%s]: %w", req.effectiveCategory(), err)
			}
			targets.DemoEval.Confirm(req.effectiveCategory())
		}
	}

	if resp.FreezeCfg != nil {
		if targets.CacheBox == nil {
			return fmt.Errorf("apply freeze_cfg: no CacheBox target")
		}
		if err := applyFreezeCfg(*resp.FreezeCfg, targets.CacheBox); err != nil {
			return fmt.Errorf("apply freeze_cfg: %w", err)
		}
	}

	return nil
}

// applyActiveFault builds a Fault from a FaultRequest and installs it on the
// DemoEvaluator. Uses the shared buildFault dispatcher.
func applyActiveFault(req FaultRequest, eval *DemoEvaluator, resolve NetworkResolver) error {
	f, err := buildFault(req, resolve)
	if err != nil {
		return err
	}

	mode := Inline
	if req.effectiveCategory() != "inline" {
		mode = Background
	}

	eval.Set(&Decision{
		Name:   "active_fault",
		Fault:  f,
		Reason: "register",
		Mode:   mode,
	}, &req)
	return nil
}

// applyFreezeCfg installs a distribution delay source on the CacheBox from a
// DelayRequest. Mirrors cachebox_admin.go's handleCacheBoxDelay.
func applyFreezeCfg(req DelayRequest, cb *CacheBox) error {
	if req.Mu < 0 {
		return fmt.Errorf("mu must be >= 0, got %f", req.Mu)
	}
	if req.Sigma < 0 {
		return fmt.Errorf("sigma must be >= 0, got %f", req.Sigma)
	}
	cb.SetDelaySource(cachebox.NewDistributionDelaySource(req.Mu, req.Sigma, req.Seed))
	return nil
}

// StartFaultWatchdog runs until ctx is cancelled. Every tick, drops any
// fault slot whose lastConfirmedAt is older than the grace period.
//
// Grace = max(3 * pollInterval, 30s). This is the only mechanism that
// protects against zombie faults when duration_ms = 0 (infinite) AND
// Manteion crashes.
func StartFaultWatchdog(ctx context.Context, eval *DemoEvaluator, pollInterval time.Duration, logger interface{ Warn(string, ...any) }) {
	grace := 3 * pollInterval
	if grace < 30*time.Second {
		grace = 30 * time.Second
	}
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, cat := range eval.StaleSlots(grace) {
				eval.ClearSlot(cat)
				if logger != nil {
					logger.Warn("watchdog: dropped stale fault slot", "category", cat, "grace", grace)
				}
			}
		}
	}
}
