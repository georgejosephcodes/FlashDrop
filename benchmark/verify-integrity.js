/**
 * benchmark/verify-integrity.js
 *
 * Zero-oversell integrity verifier.
 * Run this after npm run benchmark to prove the system never oversold.
 *
 * Usage:
 *   ITEM_ID=<uuid> TOTAL_STOCK=100 npm run verify
 *
 * Checks:
 *   1. redis_stock >= 0                          (no negative stock)
 *   2. reserved_orders <= TOTAL_STOCK            (zero overselling)
 *   3. redis_stock + reserved_orders == TOTAL_STOCK  (inventory conservation)
 *   4. No DUPLICATE order rows (idempotency held)
 */

'use strict';

require('dotenv').config();
const { Pool }  = require('pg');
const Redis     = require('ioredis');

const ITEM_ID     = process.env.ITEM_ID     || 'REPLACE_WITH_ITEM_ID';
const TOTAL_STOCK = parseInt(process.env.TOTAL_STOCK || '100', 10);

if (ITEM_ID === 'REPLACE_WITH_ITEM_ID') {
  console.error('❌  Set ITEM_ID env var: ITEM_ID=<uuid> TOTAL_STOCK=100 npm run verify');
  process.exit(1);
}

async function verify() {
  const pool  = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  const redis = new Redis(process.env.REDIS_URL);

  console.log('\n🔍  FlashDrop Integrity Check');
  console.log(`    Item ID     : ${ITEM_ID}`);
  console.log(`    Total Stock : ${TOTAL_STOCK}\n`);

  let passed = 0;
  let failed = 0;

  function pass(msg) { console.log(`  ✅  ${msg}`); passed++; }
  function fail(msg) { console.log(`  ❌  ${msg}`); failed++; }

  try {
    // ── 1. Redis live stock ──────────────────────────────────────────────
    const redisStock = parseInt(await redis.get(`item:${ITEM_ID}:stock`) || '0', 10);
    console.log(`Redis stock remaining : ${redisStock}`);

    if (redisStock >= 0) pass(`Redis stock is non-negative (${redisStock})`);
    else                  fail(`Redis stock is NEGATIVE: ${redisStock} — inventory corrupted!`);

    // ── 2. PostgreSQL order counts ───────────────────────────────────────
    const { rows: countRows } = await pool.query(
      `SELECT status, COUNT(*)::int AS count
       FROM orders
       WHERE item_id = $1
       GROUP BY status`,
      [ITEM_ID]
    );

    const counts = {};
    let totalOrders = 0;
    for (const row of countRows) {
      counts[row.status] = row.count;
      totalOrders += row.count;
    }

    const activeOrders = (counts.RESERVED || 0) + (counts.CONFIRMED || 0);

    console.log(`\nDB order breakdown:`);
    for (const [status, count] of Object.entries(counts)) {
      console.log(`  ${status.padEnd(12)}: ${count}`);
    }
    console.log(`  ${'ACTIVE'.padEnd(12)}: ${activeOrders}  (RESERVED + CONFIRMED)`);
    console.log(`  ${'TOTAL_STOCK'.padEnd(12)}: ${TOTAL_STOCK}\n`);

    // ── 3. Zero overselling ──────────────────────────────────────────────
    if (activeOrders <= TOTAL_STOCK) {
      pass(`Zero overselling — active orders (${activeOrders}) ≤ total stock (${TOTAL_STOCK})`);
    } else {
      fail(`OVERSOLD! Active orders (${activeOrders}) > total stock (${TOTAL_STOCK})`);
    }

    // ── 4. Inventory conservation ────────────────────────────────────────
    const conserved = redisStock + activeOrders;
    if (conserved === TOTAL_STOCK) {
      pass(`Inventory conserved — redis_stock (${redisStock}) + active_orders (${activeOrders}) = ${TOTAL_STOCK}`);
    } else {
      fail(`Inventory gap! redis_stock (${redisStock}) + active_orders (${activeOrders}) = ${conserved} ≠ ${TOTAL_STOCK}`);
      console.log(`    (Gap of ${Math.abs(TOTAL_STOCK - conserved)} units — likely pending holds not yet expired)`);
    }

    // ── 5. No duplicate idempotency keys ─────────────────────────────────
    const { rows: dupRows } = await pool.query(
      `SELECT idempotency_key, COUNT(*) AS cnt
       FROM orders
       WHERE item_id = $1
       GROUP BY idempotency_key
       HAVING COUNT(*) > 1`,
      [ITEM_ID]
    );

    if (dupRows.length === 0) {
      pass(`No duplicate orders — idempotency held across all ${totalOrders} DB rows`);
    } else {
      fail(`Found ${dupRows.length} duplicate idempotency key(s)!`);
    }

  } finally {
    await pool.end();
    redis.disconnect();
  }

  // ── Summary ─────────────────────────────────────────────────────────────
  console.log(`\n${'─'.repeat(48)}`);
  console.log(`Result: ${passed} passed, ${failed} failed`);
  if (failed === 0) {
    console.log(`\n🏆  All checks passed — system integrity verified!\n`);
  } else {
    console.log(`\n💥  ${failed} check(s) failed — review logs above.\n`);
    process.exit(1);
  }
}

verify().catch(err => {
  console.error('Verifier error:', err.message);
  process.exit(1);
});

