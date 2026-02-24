/**
 * BleepStore k6 Load Test
 *
 * Runs S3-compatible load tests using Grafana k6 with the k6-jslib-aws extension.
 *
 * Prerequisites:
 *   brew install k6   (or see https://grafana.com/docs/k6/latest/set-up/install-k6/)
 *
 * Usage:
 *   k6 run k6-s3.js
 *   k6 run --vus 50 --duration 60s k6-s3.js
 *   k6 run --env ENDPOINT=http://localhost:9000 k6-s3.js
 *
 * Environment variables:
 *   ENDPOINT    - BleepStore endpoint (default: http://localhost:9000)
 *   ACCESS_KEY  - Access key (default: bleepstore)
 *   SECRET_KEY  - Secret key (default: bleepstore-secret)
 *   REGION      - Region (default: us-east-1)
 *   BUCKET      - Test bucket name (default: k6-load-test)
 */

import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";
import { randomBytes } from "k6/crypto";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

// Configuration
const ENDPOINT = __ENV.ENDPOINT || "http://localhost:9000";
const ACCESS_KEY = __ENV.ACCESS_KEY || "bleepstore";
const SECRET_KEY = __ENV.SECRET_KEY || "bleepstore-secret";
const REGION = __ENV.REGION || "us-east-1";
const BUCKET = __ENV.BUCKET || "k6-load-test";

// Custom metrics
const putLatency = new Trend("s3_put_latency", true);
const getLatency = new Trend("s3_get_latency", true);
const deleteLatency = new Trend("s3_delete_latency", true);
const listLatency = new Trend("s3_list_latency", true);
const putErrors = new Rate("s3_put_errors");
const getErrors = new Rate("s3_get_errors");
const opsCounter = new Counter("s3_total_ops");

export const options = {
  scenarios: {
    // Mixed workload: 70% reads, 20% writes, 10% deletes
    mixed_load: {
      executor: "constant-vus",
      vus: 10,
      duration: "30s",
    },
  },
  thresholds: {
    s3_put_latency: ["p(95)<500"],
    s3_get_latency: ["p(95)<200"],
    s3_put_errors: ["rate<0.01"],
    s3_get_errors: ["rate<0.01"],
  },
};

// Simple SigV4-free requests using path-style URLs
// Note: k6 does not have built-in SigV4 signing. For full SigV4 tests,
// use the boto3-based benchmarks or Locust instead.
// This test uses presigned-URL-free direct requests which require the
// server to support unauthenticated access or a simpler auth mode.
//
// For authenticated load testing, prefer Locust (locustfile.py) which
// has native boto3/SigV4 support.

function putObject(key, size) {
  const data = randomBytes(size);
  const url = `${ENDPOINT}/${BUCKET}/${key}`;
  const res = http.put(url, data, {
    headers: { "Content-Type": "application/octet-stream" },
  });
  putLatency.add(res.timings.duration);
  putErrors.add(res.status !== 200);
  opsCounter.add(1);
  return res;
}

function getObject(key) {
  const url = `${ENDPOINT}/${BUCKET}/${key}`;
  const res = http.get(url);
  getLatency.add(res.timings.duration);
  getErrors.add(res.status !== 200);
  opsCounter.add(1);
  return res;
}

function deleteObject(key) {
  const url = `${ENDPOINT}/${BUCKET}/${key}`;
  const res = http.del(url);
  deleteLatency.add(res.timings.duration);
  opsCounter.add(1);
  return res;
}

function listObjects() {
  const url = `${ENDPOINT}/${BUCKET}?list-type=2&max-keys=100`;
  const res = http.get(url);
  listLatency.add(res.timings.duration);
  opsCounter.add(1);
  return res;
}

// Setup: create the test bucket
export function setup() {
  const res = http.put(`${ENDPOINT}/${BUCKET}`);
  if (res.status !== 200 && res.status !== 409) {
    console.error(`Failed to create bucket: ${res.status} ${res.body}`);
  }

  // Pre-populate with some objects
  for (let i = 0; i < 100; i++) {
    const data = randomBytes(1024);
    http.put(`${ENDPOINT}/${BUCKET}/seed-${i}`, data, {
      headers: { "Content-Type": "application/octet-stream" },
    });
  }

  return { bucket: BUCKET };
}

// Main test function
export default function () {
  const op = Math.random();

  if (op < 0.2) {
    // 20% writes
    const key = `load-test/${uuidv4()}`;
    const sizes = [1024, 10240, 102400]; // 1KB, 10KB, 100KB
    const size = sizes[Math.floor(Math.random() * sizes.length)];
    const res = putObject(key, size);
    check(res, { "PUT status is 200": (r) => r.status === 200 });
  } else if (op < 0.9) {
    // 70% reads
    const idx = Math.floor(Math.random() * 100);
    const res = getObject(`seed-${idx}`);
    check(res, { "GET status is 200": (r) => r.status === 200 });
  } else {
    // 10% list
    const res = listObjects();
    check(res, { "LIST status is 200": (r) => r.status === 200 });
  }

  sleep(0.01); // Small pause between ops
}

// Teardown: clean up
export function teardown(data) {
  // Best-effort cleanup
  for (let i = 0; i < 100; i++) {
    http.del(`${ENDPOINT}/${BUCKET}/seed-${i}`);
  }
  // Note: dynamic objects from load test are not cleaned up here
  // Use aws s3 rm --recursive for full cleanup
}
