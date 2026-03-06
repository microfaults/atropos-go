/**
 * engine.js - Config-driven flow execution engine for k6.
 *
 * Loads flow, persona, and data pool JSON configs, then provides the
 * k6 lifecycle functions (setup, run, teardown) to execute the flow.
 *
 * The engine replaces hardcoded per-service scripts with a generic
 * interpreter: adding a new service mesh = adding JSON files, zero JS.
 *
 * Execution model per VU iteration:
 *   1. Walk steps in topological order (sorted by `requires`)
 *   2. Dependency gating: if prerequisite was skipped, dependents auto-skip
 *   3. Probability gating: step probability (literal or persona key)
 *   4. Template resolution: {{pool}}, {{steps}}, {{random}}, {{env}}
 *   5. HTTP request with W3C baggage tracing
 *   6. Response check + data extraction
 *   7. Persona-based think time
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { registerWorkload, deregisterWorkload } from "./archer.js";
import { generateMetaTraceID, withTracing } from "./tracing.js";
import { resolveObject, resolveTemplate } from "./template.js";

// ── Topological sort ──────────────────────────────────────────────

/**
 * Sort steps by their `requires` dependencies. Steps without `requires`
 * come first. Detects cycles and missing dependencies.
 */
function topoSort(steps) {
  const byName = {};
  for (const s of steps) {
    byName[s.name] = s;
  }

  const sorted = [];
  const state = {}; // 'visiting' | 'done'

  function visit(step) {
    if (state[step.name] === "done") return;
    if (state[step.name] === "visiting") {
      throw new Error(`engine: cycle detected at step "${step.name}"`);
    }
    state[step.name] = "visiting";
    if (step.requires) {
      const dep = byName[step.requires];
      if (!dep) {
        throw new Error(
          `engine: step "${step.name}" requires "${step.requires}" which does not exist`
        );
      }
      visit(dep);
    }
    state[step.name] = "done";
    sorted.push(step);
  }

  for (const s of steps) {
    visit(s);
  }
  return sorted;
}

// ── Probability gate ──────────────────────────────────────────────

/**
 * Determine if a step should execute based on its probability config.
 * - No probability field → always execute
 * - Number → use directly as probability
 * - String → look up in persona.probabilities map
 */
function shouldExecute(step, persona) {
  if (step.probability === undefined || step.probability === null) return true;

  let prob;
  if (typeof step.probability === "number") {
    prob = step.probability;
  } else {
    prob = persona.probabilities[step.probability];
    if (prob === undefined) {
      // Unknown key defaults to always-execute; log would be nice but
      // k6 doesn't have console.warn in VU context without noise.
      prob = 1.0;
    }
  }
  return Math.random() < prob;
}

// ── JSON path accessor ────────────────────────────────────────────

/**
 * Simple dot-path accessor: "items.0.name" → obj.items[0].name
 */
function getJsonPath(obj, path) {
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    const idx = parseInt(part);
    current = isNaN(idx) ? current[part] : current[idx];
  }
  return current;
}

// ── Step execution ────────────────────────────────────────────────

/**
 * Execute a single flow step: resolve templates, make HTTP request,
 * check response, extract values for downstream steps.
 */
function executeStep(step, context, baseURL, traceID) {
  // Bundle ALL template expressions for this step into one resolveObject
  // call so that pool cache is shared (same pool ref → same random item).
  const templateBundle = {
    path: step.path,
    headers: step.headers || {},
    body: step.body || null,
    _extract: step.extract || {},
  };

  const resolved = resolveObject(templateBundle, context);

  const url = baseURL + resolved.path;

  // Build k6 params with W3C baggage tracing.
  const params = withTracing(
    {
      headers: resolved.headers,
      tags: { name: step.name },
    },
    traceID
  );

  // Dispatch HTTP method.
  let res;
  const method = (step.method || "GET").toUpperCase();
  const bodyStr =
    resolved.body !== null ? JSON.stringify(resolved.body) : null;

  switch (method) {
    case "GET":
      res = http.get(url, params);
      break;
    case "POST":
      res = http.post(url, bodyStr, params);
      break;
    case "PUT":
      res = http.put(url, bodyStr, params);
      break;
    case "PATCH":
      res = http.patch(url, bodyStr, params);
      break;
    case "DELETE":
      res = http.del(url, null, params);
      break;
    default:
      res = http.request(method, url, bodyStr, params);
  }

  // Check expected status codes.
  if (step.expect && step.expect.length > 0) {
    const checkName = `${step.name} status in [${step.expect.join(",")}]`;
    check(res, {
      [checkName]: (r) => step.expect.includes(r.status),
    });
  }

  // Collect extracted values for downstream steps.
  const extracted = {};
  if (step.extract) {
    for (const [key, tmpl] of Object.entries(step.extract)) {
      if (typeof tmpl === "string" && tmpl.startsWith("jsonpath:")) {
        // Extract from response body using dot-path.
        try {
          const body = JSON.parse(res.body);
          extracted[key] = getJsonPath(body, tmpl.substring("jsonpath:".length));
        } catch (_e) {
          extracted[key] = null;
        }
      } else {
        // Template expression — already resolved via the bundled resolveObject.
        extracted[key] = resolved._extract[key];
      }
    }
  }

  return { response: res, extracted: extracted };
}

// ── Engine factory ────────────────────────────────────────────────

/**
 * Create a flow execution engine. Must be called in k6 init context
 * (top-level module scope) because it uses open() to load JSON files.
 *
 * @param {string} flowPath - Path to flow JSON file
 * @param {string} personaPath - Path to persona JSON file
 * @param {string} dataPath - Path to data pool JSON file
 * @returns {object} Engine with options, setup(), run(), teardown()
 */
export function createEngine(flowPath, personaPath, dataPath) {
  // Load configs in init context (open() only works here).
  const flow = JSON.parse(open(flowPath));
  const persona = JSON.parse(open(personaPath));
  const data = JSON.parse(open(dataPath));

  // Topologically sort steps by dependencies.
  const orderedSteps = topoSort(flow.steps);

  // Build k6 options from flow config + environment overrides.
  const engineOptions = {
    vus: __ENV.VUS ? parseInt(__ENV.VUS) : 10,
    duration: __ENV.DURATION || "5m",
    thresholds: flow.thresholds || {
      http_req_failed: ["rate<0.1"],
      http_req_duration: ["p(95)<2000"],
    },
  };

  const metaTraceID = generateMetaTraceID();
  const baseURL = __ENV.BASE_URL || flow.baseUrl || "http://localhost:8080";

  return {
    flow: flow,
    persona: persona,
    data: data,
    orderedSteps: orderedSteps,
    options: engineOptions,
    metaTraceID: metaTraceID,
    baseURL: baseURL,

    /**
     * Register workload with archer. Returns setup data for VU functions.
     */
    setup: function () {
      const wl = registerWorkload({
        name: flow.name,
        profile: persona.name,
        targets: flow.targets || [],
        vus: engineOptions.vus,
        rate: engineOptions.vus * (flow.estimatedRpsPerVu || 2),
        meta_trace_id: metaTraceID,
        status: "running",
      });
      return { workloadID: wl.id, metaTraceID: metaTraceID };
    },

    /**
     * Execute one VU iteration through the flow.
     */
    run: function (setupData) {
      const traceID = setupData.metaTraceID;

      // Per-iteration state: extracted values and execution tracking.
      const stepResults = {};
      const executed = {};

      for (const step of orderedSteps) {
        // Dependency gate: skip if prerequisite was skipped.
        if (step.requires && !executed[step.requires]) {
          continue;
        }

        // Probability gate.
        if (!shouldExecute(step, persona)) {
          continue;
        }

        // Build template context for this step.
        const context = {
          pool: data,
          steps: stepResults,
          env: __ENV,
        };

        // Execute the step.
        const result = executeStep(step, context, baseURL, traceID);

        // Record execution and extracted values.
        executed[step.name] = true;
        if (result.extracted && Object.keys(result.extracted).length > 0) {
          stepResults[step.name] = result.extracted;
        }

        // Persona-based think time.
        const { min, max } = persona.thinkTime;
        sleep((min + Math.random() * (max - min)) / 1000);
      }
    },

    /**
     * Deregister workload from archer.
     */
    teardown: function (setupData) {
      deregisterWorkload(setupData.workloadID);
    },
  };
}
