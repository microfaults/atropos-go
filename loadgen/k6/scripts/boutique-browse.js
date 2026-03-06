/**
 * boutique-browse.js - Online Boutique browsing flow.
 *
 * Simulates users browsing products with persona-based behavior.
 * The persona determines how likely the user is to view products,
 * add them to cart, and how long they pause between actions.
 *
 * Usage:
 *   k6 run -e BOUTIQUE_URL=http://frontend:8080 \
 *           -e PERSONA=window-shopper \
 *           -e ARCHER_URL=http://archer:8080 \
 *           boutique-browse.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { registerWorkload, deregisterWorkload } from "./lib/archer.js";
import { getPersona, shouldAct, randomThinkTime } from "./lib/personas.js";
import { generateMetaTraceID, withTracing } from "./lib/tracing.js";

const BASE_URL = __ENV.BOUTIQUE_URL || "http://frontend:8080";
const PERSONA_NAME = __ENV.PERSONA || "window-shopper";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS) : 10,
  duration: __ENV.DURATION || "5m",
  thresholds: {
    http_req_failed: ["rate<0.1"],       // <10% errors
    http_req_duration: ["p(95)<2000"],   // p95 < 2s
  },
};

// Shared state across VUs.
const metaTraceID = generateMetaTraceID();
const persona = getPersona(PERSONA_NAME);

// Sample product IDs from Online Boutique.
const PRODUCT_IDS = [
  "OLJCESPC7Z", // sunglasses
  "66VCHSJNUP", // tank top
  "1YMWWN1N4O", // watch
  "L9ECAV7KIM", // loafers
  "2ZYFJ3GM2N", // hairdryer
  "0PUK6V6EV0", // candles
  "LS4PSXUNUM", // salt & pepper
  "9SIQT8TOJO", // bamboo glass jar
  "6E92ZMYYFZ", // mug
];

/**
 * setup() runs once before VUs start. Registers the workload with archer
 * so the policy engine knows we're generating load against these services.
 */
export function setup() {
  const wl = registerWorkload({
    name: "boutique-browse",
    profile: PERSONA_NAME,
    targets: ["frontend", "productcatalogservice", "currencyservice", "cartservice"],
    vus: options.vus,
    rate: options.vus * 2, // rough estimate: ~2 req/s per VU
    meta_trace_id: metaTraceID,
    status: "running",
  });
  return { workloadID: wl.id, metaTraceID: metaTraceID };
}

/**
 * Main VU function - each virtual user runs this in a loop.
 */
export default function (data) {
  const traceID = data.metaTraceID;

  // 1. Browse homepage
  let res = http.get(`${BASE_URL}/`, withTracing({}, traceID));
  check(res, { "homepage 200": (r) => r.status === 200 });
  sleep(randomThinkTime(persona));

  // 2. Maybe view a product
  if (shouldAct(persona.browseProbability)) {
    const productID = PRODUCT_IDS[Math.floor(Math.random() * PRODUCT_IDS.length)];
    res = http.get(
      `${BASE_URL}/product/${productID}`,
      withTracing({ tags: { name: "product_view" } }, traceID)
    );
    check(res, { "product page 200": (r) => r.status === 200 });
    sleep(randomThinkTime(persona));

    // 3. Maybe add to cart
    if (shouldAct(persona.addToCartProbability)) {
      const quantity = Math.ceil(Math.random() * 3);
      res = http.post(
        `${BASE_URL}/cart`,
        JSON.stringify({
          product_id: productID,
          quantity: quantity,
        }),
        withTracing(
          {
            headers: { "Content-Type": "application/json" },
            tags: { name: "add_to_cart" },
          },
          traceID
        )
      );
      check(res, { "add to cart": (r) => r.status === 200 || r.status === 302 });
      sleep(randomThinkTime(persona));

      // 4. Maybe view cart
      if (shouldAct(0.5)) {
        res = http.get(
          `${BASE_URL}/cart`,
          withTracing({ tags: { name: "view_cart" } }, traceID)
        );
        check(res, { "view cart 200": (r) => r.status === 200 });
        sleep(randomThinkTime(persona));
      }
    }
  }
}

/**
 * teardown() runs once after all VUs finish. Deregisters the workload.
 */
export function teardown(data) {
  deregisterWorkload(data.workloadID);
}
