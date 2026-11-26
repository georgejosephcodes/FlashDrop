/**
 * benchmark/load-test.js
 *
 * Autocannon load test — simulates a flash sale burst.
 *
 * Setup:
 *   1. docker compose up
 *   2. Seed an item with totalStock=100 via POST /api/items
 *   3. Copy the item ID into ITEM_ID below (or pass as env var)
 *   4. npm run benchmark
 *
 * What it measures:
 *   - Total throughput (req/s)
 *   - p50 / p95 / p99 latency
 *   - 2xx vs 4xx split (RESERVED vs SOLD_OUT)
 *
 * Target: p99 < 25ms, exactly 100 orders reserved, 9,900 rejected.
 */

'use strict';

const autocannon = require('autocannon');
const { v4: uuidv4 } = require('uuid');

const ITEM_ID     = process.env.ITEM_ID  || 'REPLACE_WITH_ITEM_ID';
const BASE_URL    = process.env.BASE_URL  || 'http://localhost:8080';
const CONNECTIONS = parseInt(process.env.CONNECTIONS || '200', 10);
const AMOUNT      = parseInt(process.env.AMOUNT      || '10000', 10);

if (ITEM_ID === 'REPLACE_WITH_ITEM_ID') {
  console.error('❌  Set ITEM_ID env var: ITEM_ID=<uuid> npm run benchmark');
  process.exit(1);
}

console.log(`\n🚀  FlashDrop Load Test`);
console.log(`    Target  : ${BASE_URL}`);
console.log(`    Item    : ${ITEM_ID}`);
console.log(`    Requests: ${AMOUNT}`);
console.log(`    Connections: ${CONNECTIONS}\n`);

const instance = autocannon({
  url:         `${BASE_URL}/api/orders/checkout`,
  method:      'POST',
  connections: CONNECTIONS,
  amount:      AMOUNT,
  headers: {
    'Content-Type': 'application/json',
  },
  // Each request gets a unique idempotency key + randomised userId
  setupClient(client) {
    client.setHeadersAndBody({
      headers: {
        'Content-Type':      'application/json',
        'X-Idempotency-Key': uuidv4(),
        'x-dev-user-id':     `user_${Math.floor(Math.random() * 50000)}`,
      },
      body: JSON.stringify({
        itemId:      ITEM_ID,
        amountCents: 18000,
      }),
    });
  },
}, (err, result) => {
  if (err) {
    console.error('Benchmark error:', err);
    process.exit(1);
  }

  console.log('\n📊  Results\n');
  console.log(`Throughput  : ${result.requests.average.toFixed(0)} req/s`);
  console.log(`Latency p50 : ${result.latency.p50} ms`);
  console.log(`Latency p95 : ${result.latency.p95} ms`);
  console.log(`Latency p99 : ${result.latency.p99} ms`);
  console.log(`2xx (RESERVED)  : ${result['2xx']}`);
  console.log(`4xx (SOLD_OUT)  : ${result['4xx']}`);
  console.log(`5xx (errors)    : ${result['5xx']}`);
  console.log(`Non-2xx total   : ${result.non2xx}`);
  console.log(`\nDone. Run npm run verify to check zero-oversell integrity.\n`);
});

autocannon.track(instance, { renderProgressBar: true });

