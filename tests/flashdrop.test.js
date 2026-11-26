/**
 * tests/flashdrop.test.js
 *
 * Integration tests covering FlashDrop's concurrency, idempotency,
 * authorization, payment-vs-expiry atomicity, and Write-Behind persistence.
 *
 * Requires Docker Compose stack running with SKIP_AUTH=true and
 * RATE_LIMIT_PER_SECOND=10000 (see .env / .env.local).
 */

'use strict';

require('dotenv').config({ path: '.env.local', override: false });
require('dotenv').config();

const { v4: uuidv4 } = require('uuid');
const Redis          = require('ioredis');
const { Pool }       = require('pg');

const BASE_URL = process.env.BASE_URL || 'http://localhost:8080';

jest.setTimeout(20000);

let redis;
let pool;

async function createTestItem({ totalStock = 10, priceCents = 500, saleStartAt = '2026-01-01T00:00:00Z' } = {}) {
  const res = await fetch(`${BASE_URL}/api/items`, {
    method:  'POST',
    headers: { 'content-type': 'application/json' },
    body:    JSON.stringify({
      title: `Test Drop ${uuidv4().slice(0, 8)}`,
      priceCents,
      totalStock,
      saleStartAt,
    }),
  });
  expect(res.status).toBe(201);
  return res.json();
}

async function checkoutRequest({ itemId, userId, idempotencyKey, amountCents = 500 }) {
  const res = await fetch(`${BASE_URL}/api/orders/checkout`, {
    method:  'POST',
    headers: {
      'content-type':      'application/json',
      'x-idempotency-key': idempotencyKey,
      'x-dev-user-id':     userId,
    },
    body: JSON.stringify({ itemId, amountCents }),
  });
  const body = await res.json();
  return { status: res.status, body };
}

async function payRequest({ orderId, userId }) {
  const res = await fetch(`${BASE_URL}/api/orders/${orderId}/pay`, {
    method:  'POST',
    headers: { 'x-dev-user-id': userId },
  });
  const body = await res.json();
  return { status: res.status, body };
}

async function waitForCondition(fn, timeoutMs = 8000, intervalMs = 150) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const val = await fn();
    if (val) return val;
    await new Promise(r => setTimeout(r, intervalMs));
  }
  throw new Error(`Condition timed out after ${timeoutMs}ms`);
}

beforeAll(async () => {
  redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6380');
  pool  = new Pool({ connectionString: process.env.DATABASE_URL || 'postgresql://flashdrop:flashdrop@localhost:5433/flashdrop', max: 2 });
  const res = await fetch(`${BASE_URL}/health`);
  expect(res.status).toBe(200);
});

afterAll(async () => {
  if (pool) await pool.end();
  if (redis) redis.disconnect();
});

describe('FlashDrop Integration Tests', () => {
  test('1. Last-item race: 20 concurrent requests for stock=1 → exactly 1 RESERVED, 19 SOLD_OUT', async () => {
    const item = await createTestItem({ totalStock: 1 });

    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        checkoutRequest({
          itemId:         item.id,
          userId:         `race_user_${i}`,
          idempotencyKey: uuidv4(),
        })
      )
    );

    const reserved = results.filter(r => r.status === 202);
    const soldOut  = results.filter(r => r.status === 409 && r.body.status === 'SOLD_OUT');

    expect(reserved).toHaveLength(1);
    expect(soldOut).toHaveLength(19);

    const stock = await redis.get(`item:${item.id}:stock`);
    expect(stock).toBe('0');
  });

  test('2. Idempotency replay returns same orderId without extra decrement & scopes by user', async () => {
    const item   = await createTestItem({ totalStock: 5 });
    const shared = `shared-key-${uuidv4()}`;

    const first = await checkoutRequest({ itemId: item.id, userId: 'user_alpha', idempotencyKey: shared });
    const retry = await checkoutRequest({ itemId: item.id, userId: 'user_alpha', idempotencyKey: shared });

    expect(first.status).toBe(202);
    expect(retry.status).toBe(202);
    expect(retry.body.orderId).toBe(first.body.orderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('4');

    // Different user reusing the same key gets their own order and decrements stock
    const otherUser = await checkoutRequest({ itemId: item.id, userId: 'user_beta', idempotencyKey: shared });
    expect(otherUser.status).toBe(202);
    expect(otherUser.body.orderId).not.toBe(first.body.orderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('3');
  });

  test('3. Payment rejects non-owner (403), prevents double-pay (200 + 410), and persists CONFIRMED in PG', async () => {
    const item = await createTestItem({ totalStock: 2, priceCents: 1200 });
    const res  = await checkoutRequest({
      itemId:         item.id,
      userId:         'owner_user',
      idempotencyKey: uuidv4(),
      amountCents:    1, // should be overridden by cached item.priceCents=1200
    });
    expect(res.status).toBe(202);
    const { orderId } = res.body;

    // Wait for orderProcessorWorker to create hold:{orderId}
    await waitForCondition(() => redis.get(`hold:${orderId}`));

    // Another user tries to pay -> 403 Forbidden, hold preserved
    const stealAttempt = await payRequest({ orderId, userId: 'attacker_user' });
    expect(stealAttempt.status).toBe(403);
    expect(await redis.get(`hold:${orderId}`)).not.toBeNull();

    // Owner fires two concurrent pay requests -> 1 succeeds (200), 1 rejected (410)
    const [p1, p2] = await Promise.all([
      payRequest({ orderId, userId: 'owner_user' }),
      payRequest({ orderId, userId: 'owner_user' }),
    ]);
    const statuses = [p1.status, p2.status].sort();
    expect(statuses).toEqual([200, 410]);

    // Verify dbSyncWorker persisted CONFIRMED with authoritative amountCents=1200
    const row = await waitForCondition(async () => {
      const { rows } = await pool.query('SELECT status, amount_cents FROM orders WHERE id = $1', [orderId]);
      return rows[0]?.status === 'CONFIRMED' ? rows[0] : null;
    });
    expect(row.amount_cents).toBe(1200);
  });

  test('4. Sale window enforcement blocks checkout before saleStartAt', async () => {
    const future = new Date(Date.now() + 3600_000).toISOString();
    const item   = await createTestItem({ totalStock: 3, saleStartAt: future });

    const res = await checkoutRequest({
      itemId:         item.id,
      userId:         'early_user',
      idempotencyKey: uuidv4(),
    });

    expect(res.status).toBe(403);
    expect(res.body.status).toBe('NOT_STARTED');
    expect(await redis.get(`item:${item.id}:stock`)).toBe('3');
  });

  test('5. Expired hold rejects late payment (410) and autoReclaimWorker restores stock + marks EXPIRED', async () => {
    const item = await createTestItem({ totalStock: 1 });
    const res  = await checkoutRequest({
      itemId:         item.id,
      userId:         'slow_user',
      idempotencyKey: uuidv4(),
    });
    expect(res.status).toBe(202);
    const { orderId } = res.body;

    // Wait for orderProcessorWorker to create hold:{orderId}
    await waitForCondition(() => redis.get(`hold:${orderId}`));
    expect(await redis.get(`item:${item.id}:stock`)).toBe('0');

    // Backdate hold expiry to simulate timeout elapsed before reclaim worker polls
    const pastTs = Date.now() - 1000;
    await redis.set(`hold:${orderId}`, `slow_user:${item.id}:${pastTs}`, 'EX', 3600);
    await redis.zadd('holds:expiry', pastTs, orderId);

    // Paying an expired hold is rejected atomically even before worker runs
    const latePay = await payRequest({ orderId, userId: 'slow_user' });
    expect(latePay.status).toBe(410);

    // autoReclaimWorker restores stock back to 1 and dbSyncWorker marks PG EXPIRED
    await waitForCondition(async () => (await redis.get(`item:${item.id}:stock`)) === '1');
    const row = await waitForCondition(async () => {
      const { rows } = await pool.query('SELECT status FROM orders WHERE id = $1', [orderId]);
      return rows[0]?.status === 'EXPIRED' ? rows[0] : null;
    });
    expect(row.status).toBe('EXPIRED');
  });
});

