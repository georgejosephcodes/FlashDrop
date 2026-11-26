/**
 * benchmark/load-test.js
 *
 * Autocannon load test — simulates a flash sale burst.
 *
 * Usage:
 *   ITEM_ID=<uuid> npm run benchmark
 *   ITEM_ID=<uuid> CONNECTIONS=200 AMOUNT=10000 npm run benchmark
 *
 * Uses Autocannon's built-in `idReplacement` ([<id>]) so every single request
 * across all concurrent connections gets a unique X-Idempotency-Key and
 * unique userId — ensuring zero idempotency-cache shortcuts.
 */

'use strict';

const autocannon = require('autocannon');

const ITEM_ID     = process.env.ITEM_ID     || 'REPLACE_WITH_ITEM_ID';
const BASE_URL    = process.env.BASE_URL    || 'http://localhost:8080';
const CONNECTIONS = parseInt(process.env.CONNECTIONS || '200', 10);
const AMOUNT      = parseInt(process.env.AMOUNT      || '10000', 10);

if (ITEM_ID === 'REPLACE_WITH_ITEM_ID') {
  console.error('❌  Set ITEM_ID env var: ITEM_ID=<uuid> npm run benchmark');
  process.exit(1);
}

console.log(`\n🚀  FlashDrop Load Test`);
console.log(`    Target     : ${BASE_URL}`);
console.log(`    Item       : ${ITEM_ID}`);
console.log(`    Requests   : ${AMOUNT}`);
console.log(`    Connections: ${CONNECTIONS}\n`);

const instance = autocannon({
  url:           `${BASE_URL}/api/orders/checkout`,
  method:        'POST',
  connections:   CONNECTIONS,
  amount:        AMOUNT,
  idReplacement: true,
  headers: {
    'content-type':      'application/json',
    'x-idempotency-key': '[<id>]',
    'x-dev-user-id':     'user_[<id>]',
  },
  body: JSON.stringify({ itemId: ITEM_ID, amountCents: 18000 }),
}, (err, result) => {
  if (err) {
    console.error('Benchmark error:', err);
    process.exit(1);
  }

  const p95 = result.latency.p97_5 || result.latency['97.5'] || 'N/A';

  console.log('\n📊  Results\n');
  console.log(`Throughput      : ${result.requests.average.toFixed(0)} req/s`);
  console.log(`Latency p50     : ${result.latency.p50} ms`);
  console.log(`Latency p95     : ${p95} ms`);
  console.log(`Latency p99     : ${result.latency.p99} ms`);
  console.log(`2xx (RESERVED)  : ${result['2xx']}`);
  console.log(`4xx (SOLD_OUT)  : ${result['4xx']}`);
  console.log(`5xx (errors)    : ${result['5xx']}`);
  console.log(`\nDone. Run npm run verify to check zero-oversell integrity.\n`);
});

autocannon.track(instance, { renderProgressBar: true });
