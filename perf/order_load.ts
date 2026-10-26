// k6 load test for POST /orders. k6 >= 1.0 runs TypeScript natively (types are stripped).
import http from "k6/http";
import { check } from "k6";
import type { Options } from "k6/options";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8000";
const RATE = Number(__ENV.RATE || 1000); // requests per second
const DURATION = __ENV.DURATION || "60s";
const P99_MS = Number(__ENV.P99_MS || 25); // see README "Performance baseline" for how this was set

const SYMBOLS = ["AAPL", "MSFT", "NVDA", "AMZN"];
const ACCOUNTS = 50;

export const options: Options = {
  scenarios: {
    orders: {
      // Open model: new requests start on schedule even if earlier ones are slow, so server
      // slowness shows up as latency instead of quietly lowering the load (coordinated omission).
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: [`p(99)<${P99_MS}`],
    checks: ["rate>0.99"],
    // If k6 runs out of VUs it skips iterations; that would mean we did not offer the target rate.
    dropped_iterations: ["count==0"],
  },
  summaryTrendStats: ["avg", "med", "p(90)", "p(95)", "p(99)", "max"],
};

export default function (): void {
  const order = {
    order_id: crypto.randomUUID(),
    client_order_id: `k6-${__VU}-${__ITER}`,
    account: `PERF-${Math.floor(Math.random() * ACCOUNTS)}`,
    symbol: SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)],
    side: Math.random() < 0.5 ? "BUY" : "SELL",
    qty: 1 + Math.floor(Math.random() * 100),
    price: 100.25,
    ts: new Date().toISOString(),
  };
  const res = http.post(`${BASE_URL}/orders`, JSON.stringify(order), {
    headers: { "Content-Type": "application/json", "X-Correlation-ID": `k6-${order.order_id}` },
  });
  check(res, { "status is 202": (r) => r.status === 202 });
}
