package wire

// RuleSync is the desired-state payload manteion delivers to SDKs — the body
// of a 200 poll response (GET /api/v1/sdk/rules) and, embedded in
// RegisterResponse, the intent piggybacked on registration. Both ends
// marshal/unmarshal this one struct, so the field set cannot drift between
// control plane and SDK.
//
// Rules and ActiveFaults are deliberately NOT omitempty: under apply's
// reconciliation an empty array means "clear everything in this category",
// which must be explicit on the wire rather than indistinguishable from
// "field absent". A null/absent rules field (nil after decode) means "no
// change" — the compatibility escape hatch for payloads that don't carry
// rules — while [] is authoritative desired state. Manteion's poll
// endpoint always sends the full (possibly empty) compiled set.
// Recording/replay provenance rides each matched rule's CacheBoxContext
// (design doc Q5/INV-5); there is no ambient recording-phase signal here.
type RuleSync struct {
	Version uint64 `json:"version"` // the generation, kept under the old name
	// Etag is the ETag of this materialized document: a hash of its content
	// alone, never the generation (that travels in Version and in the
	// X-Atropos-Generation header), so an unchanged document keeps its ETag
	// while the generation moves. The instance treats it as opaque and sends
	// it back as `If-None-Match`.
	Etag         string         `json:"etag,omitempty"`
	Rules        []CompiledRule `json:"rules"`
	ActiveFaults []FaultRequest `json:"active_faults"`
	FreezeCfg    *DelayRequest  `json:"freeze_cfg,omitempty"`
	// ReplaySet is the replay set desired on this instance: absent means no replay
	// set is desired, so the instance clears one it holds. Present, it names the
	// set to pull: ExperimentID and PhaseID are the (experiment, phase) pair it
	// is installed under (the cache-box serves it only to the rule whose context
	// matches that pair), ID is `sha256:<hex>` of the artifact bytes fetched from
	// URL, and SHA256 is the W5 digest over its entries.
	ReplaySet *ReplaySetRef `json:"replay_set,omitempty"`
} //@name atropos.RuleSync

// ReplaySetRef names a replay set manteion wants installed on this instance.
// ExperimentID and PhaseID name the (experiment, phase) pair the set is
// installed under: the cache-box serves it only to the rule whose context
// matches that pair. ID is `sha256:<hex>` of the artifact bytes, which the
// instance fetches from URL; SHA256 is the W5 digest over its entries.
type ReplaySetRef struct {
	ID           string `json:"id"`
	ExperimentID string `json:"experiment_id"`
	PhaseID      string `json:"phase_id"`
	KeyStrategy  string `json:"key_strategy"`
	Count        int    `json:"count"`
	SHA256       string `json:"sha256"` // W5 digest over entries (cachebox.SetChecksum)
	SizeBytes    int64  `json:"size_bytes"`
	URL          string `json:"url"` // relative to the manteion base URL
} //@name atropos.ReplaySetRef

// UnsupportedRule is one rule the instance skipped, as reported in StatusReport.
type UnsupportedRule struct {
	Rule     string `json:"rule"`
	Category string `json:"category"`
} //@name atropos.UnsupportedRule

// ReplaySetStatus names the desired replay set (ID) and its pull state. Only
// committed means the instance holds it: fetching and failed name a set not yet
// held, while an earlier committed set may still be installed.
type ReplaySetStatus struct {
	ID    string `json:"id"`
	State string `json:"state"` // fetching | committed | failed
} //@name atropos.ReplaySetStatus

// StatusReport is the instance's full snapshot of what it holds, sent after
// every apply and whenever the replay-set state changes. Unsupported and Errors
// replace the previous report, so they are never omitted: an empty list clears
// it. ReplaySet is the desired replay set and its pull state; absent means
// none is desired.
type StatusReport struct {
	InstanceID        string            `json:"instance_id"`
	Service           string            `json:"service"`
	AppliedGeneration uint64            `json:"applied_generation"`
	Etag              string            `json:"etag"`
	Unsupported       []UnsupportedRule `json:"unsupported"`
	Errors            []string          `json:"errors"`
	ReplaySet         *ReplaySetStatus  `json:"replay_set,omitempty"`
} //@name atropos.StatusReport

// Replay-set states carried in ReplaySetStatus.State.
const (
	ReplaySetFetching  = "fetching"
	ReplaySetCommitted = "committed"
	ReplaySetFailed    = "failed"
)
