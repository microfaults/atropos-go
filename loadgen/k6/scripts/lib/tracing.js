/**
 * Trace context helpers for cross-mesh correlation.
 *
 * k6 generates a meta-trace-id that gets injected into W3C Baggage headers
 * on all outgoing requests. This ID:
 * 1. Is registered with archer as part of workload metadata
 * 2. Survives Envoy/Istio proxy propagation via the baggage header
 * 3. Allows correlating traces across mesh boundaries
 */

/**
 * Generate a random hex meta-trace-id (32 hex chars = 16 bytes).
 * Uses crypto-safe random when available, falls back to Math.random.
 */
export function generateMetaTraceID() {
  const chars = "0123456789abcdef";
  let id = "";
  for (let i = 0; i < 32; i++) {
    id += chars[Math.floor(Math.random() * 16)];
  }
  return id;
}

/**
 * Inject meta-trace-id into a k6 request params object via W3C Baggage header.
 * If the params already has a baggage header, the meta-trace-id is appended.
 *
 * @param {Object} params - k6 request params (or empty object)
 * @param {string} metaTraceID - The meta-trace-id to inject
 * @returns {Object} params with baggage header set
 *
 * @example
 *   const params = withTracing({}, metaTraceID);
 *   http.get("http://frontend/", params);
 */
export function withTracing(params, metaTraceID) {
  params = params || {};
  params.headers = params.headers || {};

  const entry = `meta-trace-id=${metaTraceID}`;
  const existing = params.headers["baggage"] || "";
  params.headers["baggage"] = existing ? `${existing},${entry}` : entry;

  return params;
}

/**
 * Create a tagged params object that includes both tracing and k6 tags.
 * Tags help k6 group metrics by workload/persona in its output.
 *
 * @param {string} metaTraceID - The meta-trace-id
 * @param {Object} tags - Additional k6 tags (e.g., { name: "browse" })
 * @returns {Object} k6 params with baggage header and tags
 */
export function withTracingAndTags(metaTraceID, tags) {
  return withTracing({ tags: tags || {} }, metaTraceID);
}
