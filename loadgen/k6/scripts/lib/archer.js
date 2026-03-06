import http from "k6/http";
import { check } from "k6";

// ARCHER_URL is the base URL of the archer Go service.
// Override via environment variable: k6 run -e ARCHER_URL=http://host:port ...
const ARCHER_URL = __ENV.ARCHER_URL || "http://localhost:8080";

/**
 * Register an active workload with archer so it knows which services
 * are under load and can target them for precision attacks.
 *
 * @param {Object} workload - Workload metadata
 * @param {string} workload.name - Workload name (e.g., "boutique-browse")
 * @param {string} workload.profile - User persona (e.g., "window-shopper")
 * @param {string[]} workload.targets - Services being hit
 * @param {number} workload.vus - Number of virtual users
 * @param {number} workload.rate - Estimated req/s
 * @param {string} workload.meta_trace_id - Cross-mesh trace correlation ID
 * @param {string} workload.status - Workload status
 * @returns {Object} The registered workload with server-assigned ID
 */
export function registerWorkload(workload) {
  const res = http.post(
    `${ARCHER_URL}/api/v1/workloads`,
    JSON.stringify(workload),
    { headers: { "Content-Type": "application/json" } }
  );
  check(res, {
    "workload registered (201)": (r) => r.status === 201,
  });
  return JSON.parse(res.body);
}

/**
 * Deregister a workload when the k6 test finishes.
 *
 * @param {string} id - The workload ID returned by registerWorkload
 */
export function deregisterWorkload(id) {
  const res = http.del(`${ARCHER_URL}/api/v1/workloads/${id}`);
  check(res, {
    "workload deregistered (204)": (r) => r.status === 204,
  });
}

/**
 * Trigger a targeted vegeta attack via archer.
 *
 * @param {Object} config - Attack configuration
 * @returns {Object} The attack object with status
 */
export function triggerAttack(config) {
  const res = http.post(
    `${ARCHER_URL}/api/v1/attacks`,
    JSON.stringify(config),
    { headers: { "Content-Type": "application/json" } }
  );
  check(res, {
    "attack accepted (202)": (r) => r.status === 202,
  });
  return JSON.parse(res.body);
}

/**
 * Check the status of a running attack.
 *
 * @param {string} id - Attack ID
 * @returns {Object} Attack status and results
 */
export function getAttackStatus(id) {
  const res = http.get(`${ARCHER_URL}/api/v1/attacks/${id}`);
  return JSON.parse(res.body);
}
