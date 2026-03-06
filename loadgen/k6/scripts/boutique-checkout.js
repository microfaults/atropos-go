/**
 * boutique-checkout.js - Online Boutique full purchase flow.
 *
 * Simulates users who browse, add to cart, and complete checkout.
 * Defaults to the "spendthrift" persona (high purchase rate).
 *
 * This flow exercises the full service chain:
 *   frontend -> productcatalog -> cart -> checkout -> payment -> shipping -> email
 *
 * Usage:
 *   k6 run -e BOUTIQUE_URL=http://frontend:8080 \
 *           -e PERSONA=spendthrift \
 *           -e ARCHER_URL=http://archer:8080 \
 *           boutique-checkout.js
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { registerWorkload, deregisterWorkload } from "./lib/archer.js";
import { getPersona, shouldAct, randomThinkTime } from "./lib/personas.js";
import { generateMetaTraceID, withTracing } from "./lib/tracing.js";

const BASE_URL = __ENV.BOUTIQUE_URL || "http://frontend:8080";
const PERSONA_NAME = __ENV.PERSONA || "spendthrift";

export const options = {
  vus: __ENV.VUS ? parseInt(__ENV.VUS) : 5,
  duration: __ENV.DURATION || "5m",
  thresholds: {
    http_req_failed: ["rate<0.1"],
    http_req_duration: ["p(95)<3000"],
  },
};

const metaTraceID = generateMetaTraceID();
const persona = getPersona(PERSONA_NAME);

const PRODUCT_IDS = [
  "OLJCESPC7Z",
  "66VCHSJNUP",
  "1YMWWN1N4O",
  "L9ECAV7KIM",
  "2ZYFJ3GM2N",
];

export function setup() {
  const wl = registerWorkload({
    name: "boutique-checkout",
    profile: PERSONA_NAME,
    targets: [
      "frontend",
      "productcatalogservice",
      "cartservice",
      "checkoutservice",
      "paymentservice",
      "shippingservice",
      "emailservice",
      "currencyservice",
    ],
    vus: options.vus,
    rate: options.vus * 3,
    meta_trace_id: metaTraceID,
    status: "running",
  });
  return { workloadID: wl.id, metaTraceID: metaTraceID };
}

export default function (data) {
  const traceID = data.metaTraceID;

  // 1. Browse homepage
  let res = http.get(`${BASE_URL}/`, withTracing({}, traceID));
  check(res, { "homepage 200": (r) => r.status === 200 });
  sleep(randomThinkTime(persona));

  // 2. Browse a product
  const productID = PRODUCT_IDS[Math.floor(Math.random() * PRODUCT_IDS.length)];
  if (shouldAct(persona.browseProbability)) {
    res = http.get(
      `${BASE_URL}/product/${productID}`,
      withTracing({ tags: { name: "product_view" } }, traceID)
    );
    check(res, { "product page 200": (r) => r.status === 200 });
    sleep(randomThinkTime(persona));
  }

  // 3. Add to cart
  if (shouldAct(persona.addToCartProbability)) {
    res = http.post(
      `${BASE_URL}/cart`,
      JSON.stringify({
        product_id: productID,
        quantity: Math.ceil(Math.random() * 2),
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

    // 4. View cart
    res = http.get(
      `${BASE_URL}/cart`,
      withTracing({ tags: { name: "view_cart" } }, traceID)
    );
    check(res, { "view cart 200": (r) => r.status === 200 });
    sleep(randomThinkTime(persona));

    // 5. Checkout
    if (shouldAct(persona.checkoutProbability)) {
      res = http.post(
        `${BASE_URL}/cart/checkout`,
        JSON.stringify({
          email: "test@loadgen.local",
          street_address: "1600 Amphitheatre Parkway",
          zip_code: "94043",
          city: "Mountain View",
          state: "CA",
          country: "United States",
          credit_card_number: "4432-8015-6152-0454",
          credit_card_expiration_month: 1,
          credit_card_expiration_year: 2030,
          credit_card_cvv: 672,
        }),
        withTracing(
          {
            headers: { "Content-Type": "application/json" },
            tags: { name: "checkout" },
          },
          traceID
        )
      );
      check(res, {
        "checkout success": (r) => r.status === 200 || r.status === 302,
      });
      sleep(randomThinkTime(persona));
    }
  }
}

export function teardown(data) {
  deregisterWorkload(data.workloadID);
}
