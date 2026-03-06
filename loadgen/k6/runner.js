/**
 * runner.js - Generic config-driven k6 flow runner.
 *
 * This is a thin entry point that loads flow, persona, and data pool
 * JSON configs and delegates all execution to the flow engine.
 *
 * Adding a new service mesh = new JSON folder under flows/. Zero JS code.
 *
 * Usage:
 *   k6 run -e BASE_URL=http://frontend:8080 \
 *          -e FLOW=online-boutique/browse \
 *          -e PERSONA=cautious \
 *          -e ARCHER_URL=http://archer:8080 \
 *          runner.js
 *
 * Environment variables:
 *   FLOW       Path relative to flows/ dir (e.g., "online-boutique/browse")
 *   PERSONA    Persona filename without .json (e.g., "cautious", "aggressive")
 *   DATA       Optional data pool path override (defaults to flows/<app>/data.json)
 *   BASE_URL   Target service base URL
 *   VUS        Virtual users (default: 10)
 *   DURATION   Test duration (default: "5m")
 *   ARCHER_URL Archer service URL for workload registration
 */

import { createEngine } from "./scripts/lib/engine.js";

// ── Derive config file paths from environment variables ───────────

const FLOW = __ENV.FLOW || "online-boutique/browse";
const PERSONA = __ENV.PERSONA || "cautious";

const flowPath = `./flows/${FLOW}.json`;
const personaPath = `./personas/${PERSONA}.json`;

// Data pool: defaults to flows/<app-directory>/data.json
// Override with DATA env var for non-standard layouts.
const flowDir = FLOW.substring(0, FLOW.lastIndexOf("/"));
const dataFile = __ENV.DATA || `${flowDir}/data`;
const dataPath = `./flows/${dataFile}.json`;

// ── Create engine in init context (open() works here) ─────────────

const engine = createEngine(flowPath, personaPath, dataPath);

// ── Export k6 lifecycle hooks ─────────────────────────────────────

export const options = engine.options;

export function setup() {
  return engine.setup();
}

export default function (data) {
  engine.run(data);
}

export function teardown(data) {
  engine.teardown(data);
}
