/**
 * User persona definitions for workload simulation.
 *
 * Each persona defines probabilistic behavior for a simulated user:
 * - browseProbability: chance of viewing a product page
 * - addToCartProbability: chance of adding viewed product to cart
 * - checkoutProbability: chance of completing checkout after adding to cart
 * - thinkTimeMs: range of pause between actions (simulates reading/thinking)
 */
export const PERSONAS = {
  "window-shopper": {
    name: "window-shopper",
    description: "Browses heavily but rarely buys",
    browseProbability: 0.9,
    addToCartProbability: 0.2,
    checkoutProbability: 0.05,
    thinkTimeMs: { min: 2000, max: 5000 },
  },
  spendthrift: {
    name: "spendthrift",
    description: "Quick decisions, high purchase rate",
    browseProbability: 0.5,
    addToCartProbability: 0.8,
    checkoutProbability: 0.7,
    thinkTimeMs: { min: 500, max: 1500 },
  },
  "bargain-hunter": {
    name: "bargain-hunter",
    description: "Browses everything, compares, buys selectively",
    browseProbability: 0.95,
    addToCartProbability: 0.4,
    checkoutProbability: 0.3,
    thinkTimeMs: { min: 3000, max: 8000 },
  },
};

/**
 * Get a persona by name, falling back to window-shopper.
 */
export function getPersona(name) {
  return PERSONAS[name] || PERSONAS["window-shopper"];
}

/**
 * Probabilistic decision: returns true with the given probability.
 */
export function shouldAct(probability) {
  return Math.random() < probability;
}

/**
 * Random think time within the persona's range, in seconds (for k6 sleep()).
 */
export function randomThinkTime(persona) {
  const { min, max } = persona.thinkTimeMs;
  return (min + Math.random() * (max - min)) / 1000;
}
