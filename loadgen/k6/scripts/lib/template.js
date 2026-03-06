/**
 * template.js - Lightweight {{expression}} parser and evaluator.
 *
 * Evaluates template expressions against a context containing:
 *   - pool: data pools (named collections of objects)
 *   - steps: extracted values from previously executed flow steps
 *   - env: k6 __ENV variables
 *
 * Expression syntax:
 *   {{pool.<collection>.<field>}}    - Random item from pool, access field
 *   {{steps.<step_name>.<key>}}      - Value from a previous step's extract
 *   {{random_int(min,max)}}          - Random integer in [min, max]
 *   {{random_choice(a,b,c)}}         - Random pick from comma-separated literals
 *   {{env.VAR_NAME}}                 - k6 __ENV variable
 *
 * Pool cache: within a single resolveObject() call, repeated references to
 * the same pool collection return the same random item. This ensures that
 * {{pool.products.id}} in a URL and {{pool.products.name}} in the body
 * of the same step refer to the same product.
 */

const EXPR_RE = /\{\{(.+?)\}\}/g;

/**
 * Evaluate a single template expression against the context.
 *
 * @param {string} expr - The expression inside {{...}}, trimmed
 * @param {object} context - { pool, steps, env, _poolCache }
 * @returns {*} The resolved value (may be string, number, object, etc.)
 */
function evaluateExpression(expr, context) {
  expr = expr.trim();

  // pool.<collection>.<field>
  if (expr.startsWith("pool.")) {
    const parts = expr.split(".");
    const collection = parts[1];
    const field = parts.slice(2).join(".");

    if (!context._poolCache[collection]) {
      const items = context.pool[collection];
      if (!items || items.length === 0) {
        throw new Error(`template: pool "${collection}" is empty or undefined`);
      }
      context._poolCache[collection] =
        items[Math.floor(Math.random() * items.length)];
    }

    const item = context._poolCache[collection];
    // Support nested field access: "address.city"
    return getNestedField(item, field);
  }

  // steps.<step_name>.<key>
  if (expr.startsWith("steps.")) {
    const parts = expr.split(".");
    const stepName = parts[1];
    const key = parts.slice(2).join(".");
    const stepData = context.steps[stepName];
    if (!stepData) {
      return undefined;
    }
    return getNestedField(stepData, key);
  }

  // random_int(min,max)
  const intMatch = expr.match(/^random_int\((\d+),\s*(\d+)\)$/);
  if (intMatch) {
    const min = parseInt(intMatch[1]);
    const max = parseInt(intMatch[2]);
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  // random_choice(a,b,c)
  const choiceMatch = expr.match(/^random_choice\((.+)\)$/);
  if (choiceMatch) {
    const choices = choiceMatch[1].split(",").map((s) => s.trim());
    return choices[Math.floor(Math.random() * choices.length)];
  }

  // env.VAR_NAME
  if (expr.startsWith("env.")) {
    const varName = expr.substring(4);
    return __ENV[varName] || "";
  }

  throw new Error(`template: unknown expression "${expr}"`);
}

/**
 * Access a nested field on an object via dot-separated path.
 * "address.city" on { address: { city: "NYC" } } returns "NYC".
 */
function getNestedField(obj, path) {
  if (!path) return obj;
  const parts = path.split(".");
  let current = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    const idx = parseInt(part);
    current = isNaN(idx) ? current[part] : current[idx];
  }
  return current;
}

/**
 * Resolve a single template string. If the entire string is one expression,
 * returns the native type (number, object, etc.). Otherwise performs string
 * interpolation.
 *
 * @param {string} templateStr - String possibly containing {{...}} expressions
 * @param {object} context - { pool, steps, env, _poolCache }
 * @returns {*} Resolved value
 */
export function resolveTemplate(templateStr, context) {
  if (typeof templateStr !== "string") return templateStr;

  // Pass through jsonpath: prefixed strings for post-request processing
  if (templateStr.startsWith("jsonpath:")) return templateStr;

  // If the entire string is a single expression, return its native type
  const fullMatch = templateStr.match(/^\{\{(.+?)\}\}$/);
  if (fullMatch && templateStr.indexOf("{{", 2) === -1) {
    return evaluateExpression(fullMatch[1], context);
  }

  // String interpolation: replace all {{...}} with stringified values
  return templateStr.replace(EXPR_RE, (_, expr) => {
    const value = evaluateExpression(expr, context);
    return value !== undefined ? String(value) : "";
  });
}

/**
 * Deep-resolve all template expressions in an object/array/string.
 * Creates a fresh pool cache so that pool references within this call
 * are consistent (same collection → same random item).
 *
 * @param {*} obj - Value to resolve (string, object, array, or primitive)
 * @param {object} context - { pool, steps, env }
 * @returns {*} Deep copy with all templates resolved
 */
export function resolveObject(obj, context) {
  const scopedContext = {
    pool: context.pool,
    steps: context.steps,
    env: context.env,
    _poolCache: {},
  };
  return resolveDeep(obj, scopedContext);
}

function resolveDeep(obj, context) {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === "string") return resolveTemplate(obj, context);
  if (Array.isArray(obj)) return obj.map((item) => resolveDeep(item, context));
  if (typeof obj === "object") {
    const result = {};
    for (const key of Object.keys(obj)) {
      result[key] = resolveDeep(obj[key], context);
    }
    return result;
  }
  return obj; // numbers, booleans pass through
}
