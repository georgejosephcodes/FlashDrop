/**
 * tests/flashdrop.test.js
 *
 * Integration tests covering FlashDrop's concurrency, atomic Lua transitions,
 * idempotency edge cases (>30s slow request, crash expiry, user scoping),
 * authorization, payment-vs-expiry atomicity, and XAUTOCLAIM DB recovery.
 *
 * Requires Docker Compose stack running with SKIP_AUTH=true and
 * RATE_LIMIT_PER_SECOND=10000 (see .env / .env.local).
 */

'use strict';

require('dotenv').config({ path: '.env.local', override: false });
require('dotenv').config();

const http           = require('http');
const express        = require('express');
const { v4: uuidv4 } = require('uuid');
const Redis          = require('ioredis');
const { Pool }       = require('pg');

const BASE_URL = process.env.BASE_URL || 'http://localhost:8080';

jest.setTimeout(25000);

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
  pool  = new Pool({
    connectionString: process.env.DATABASE_URL || 'postgresql://flashdrop:flashdrop@localhost:5433/flashdrop',
    max: 2,
  });
  const res = await fetch(`${BASE_URL}/health`);
  expect(res.status).toBe(200);
});

afterAll(async () => {
  if (pool) await pool.end();
  if (redis) redis.disconnect();
  const { getRedisClient } = require('../src/config/redis');
  const { pool: appPool }  = require('../src/config/db');
  await appPool.end().catch(() => {});
  getRedisClient().disconnect();
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

  test('2. Idempotency: concurrent duplicate returns 202 + 409/202, replay works, and different users are isolated', async () => {
    const item   = await createTestItem({ totalStock: 5 });
    const shared = `shared-key-${uuidv4()}`;

    // Two simultaneous requests with the same user and key
    const [c1, c2] = await Promise.all([
      checkoutRequest({ itemId: item.id, userId: 'user_alpha', idempotencyKey: shared }),
      checkoutRequest({ itemId: item.id, userId: 'user_alpha', idempotencyKey: shared }),
    ]);

    const successes = [c1, c2].filter(r => r.status === 202);
    const conflicts = [c1, c2].filter(r => r.status === 409);
    expect(successes.length).toBeGreaterThanOrEqual(1);
    expect(successes.length + conflicts.length).toBe(2);
    if (successes.length === 2) {
      expect(c1.body.orderId).toBe(c2.body.orderId);
    }
    expect(await redis.get(`item:${item.id}:stock`)).toBe('4');

    // Replay after completion returns the original orderId without decrementing stock
    await new Promise(r => setTimeout(r, 100));
    const retry = await checkoutRequest({ itemId: item.id, userId: 'user_alpha', idempotencyKey: shared });
    expect(retry.status).toBe(202);
    expect(retry.body.orderId).toBe(successes[0].body.orderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('4');

    // Different user reusing the same key gets their own order and decrements stock
    const otherUser = await checkoutRequest({ itemId: item.id, userId: 'user_beta', idempotencyKey: shared });
    expect(otherUser.status).toBe(202);
    expect(otherUser.body.orderId).not.toBe(successes[0].body.orderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('3');
  });

  test('3. Payment rejects non-owner (403), prevents double-pay (200 + 410), and atomically writes OrderConfirmed to PG', async () => {
    const item = await createTestItem({ totalStock: 2, priceCents: 1200 });
    const res  = await checkoutRequest({
      itemId:         item.id,
      userId:         'owner_user',
      idempotencyKey: uuidv4(),
      amountCents:    1, // overridden by server-cached item.priceCents=1200
    });
    expect(res.status).toBe(202);
    const { orderId } = res.body;

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

  test('5. Expired hold rejects late payment (410) and autoReclaimWorker atomically restores stock + marks EXPIRED', async () => {
    const item = await createTestItem({ totalStock: 1 });
    const res  = await checkoutRequest({
      itemId:         item.id,
      userId:         'slow_user',
      idempotencyKey: uuidv4(),
    });
    expect(res.status).toBe(202);
    const { orderId } = res.body;

    await waitForCondition(() => redis.get(`hold:${orderId}`));
    expect(await redis.get(`item:${item.id}:stock`)).toBe('0');

    // Backdate hold expiry to simulate timeout elapsed before reclaim worker polls
    const pastTs = Date.now() - 1000;
    await redis.set(`hold:${orderId}`, `slow_user:${item.id}:${pastTs}`, 'EX', 3600);
    await redis.zadd('holds:expiry', pastTs, orderId);

    const latePay = await payRequest({ orderId, userId: 'slow_user' });
    expect(latePay.status).toBe(410);

    await waitForCondition(async () => (await redis.get(`item:${item.id}:stock`)) === '1');
    const row = await waitForCondition(async () => {
      const { rows } = await pool.query('SELECT status FROM orders WHERE id = $1', [orderId]);
      return rows[0]?.status === 'EXPIRED' ? rows[0] : null;
    });
    expect(row.status).toBe('EXPIRED');
  });

  test('6. DB sync XAUTOCLAIM recovers pending entries belonging to a different dead consumer', async () => {
    const item      = await createTestItem({ totalStock: 1 });
    const orderId   = uuidv4();
    const streamKey = `test:orders:stream:${uuidv4()}`;
    const groupName = 'testDbSyncers';

    await redis.xgroup('CREATE', streamKey, groupName, '0', 'MKSTREAM');
    await redis.xadd(
      streamKey, '*',
      'event', 'OrderReserved',
      'orderId', orderId,
      'userId', 'crashed_worker_user',
      'itemId', item.id,
      'amountCents', '500',
      'idempotencyKey', `crashed_worker_user:${uuidv4()}`,
      'timestamp', String(Date.now())
    );

    // Simulate an old consumer PID reading the entry and crashing before XACK
    const readByDead = await redis.xreadgroup(
      'GROUP', groupName, 'dbsync-dead-111',
      'COUNT', 1,
      'STREAMS', streamKey, '>'
    );
    expect(readByDead?.[0]?.[1]).toHaveLength(1);

    // Wait past idle threshold (150ms for test) and run recovery as a new consumer PID
    await new Promise(r => setTimeout(r, 200));

    const { setOnFlushed }        = require('../src/services/syncService');
    const { reclaimStalePending } = require('../src/workers/dbSyncWorker');

    try {
      setOnFlushed(async (streamIds) => {
        await redis.xack(streamKey, groupName, ...streamIds);
      });

      await reclaimStalePending(redis, 150, {
        streamKey,
        groupName,
        consumer: 'dbsync-new-222',
      });

      const { rows } = await pool.query('SELECT status FROM orders WHERE id = $1', [orderId]);
      expect(rows).toHaveLength(1);
      expect(rows[0].status).toBe('RESERVED');

      const pendingAfter = await redis.xpending(streamKey, groupName);
      expect(pendingAfter[0]).toBe(0);
    } finally {
      await redis.del(streamKey);
    }
  });

  test('7. Idempotency pending lease renews while in-flight (>TTL) and expires after API crash', async () => {
    process.env.IDEMPOTENCY_PENDING_TTL_SECONDS = '2';
    process.env.SKIP_AUTH = 'true';

    let idempotency;
    let getRedisClient;
    jest.isolateModules(() => {
      ({ idempotency }    = require('../src/middleware/idempotency'));
      ({ getRedisClient } = require('../src/config/redis'));
    });

    let releaseSlowHandler;
    let handlerCalls = 0;

    const app = express();
    app.use(express.json());
    app.post('/slow', idempotency, async (_req, res) => {
      handlerCalls++;
      await new Promise(resolve => { releaseSlowHandler = resolve; });
      return res.status(202).json({ ok: true, call: handlerCalls });
    });

    const server = http.createServer(app);
    await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
    const port   = server.address().port;
    const url    = `http://127.0.0.1:${port}/slow`;

    try {
      const slowKey = `slow-${uuidv4()}`;

      // Start slow request (> 2s pending TTL)
      const slowReqPromise = fetch(url, {
        method:  'POST',
        headers: { 'x-idempotency-key': slowKey, 'x-dev-user-id': 'slow_lease_user' },
      });

      // Wait 2.6s (exceeds initial 2s TTL — heartbeat at ~0.66s/1.3s/2s keeps lock alive)
      await new Promise(r => setTimeout(r, 2600));

      const dupWhileInFlight = await fetch(url, {
        method:  'POST',
        headers: { 'x-idempotency-key': slowKey, 'x-dev-user-id': 'slow_lease_user' },
      });
      expect(dupWhileInFlight.status).toBe(409);
      expect(handlerCalls).toBe(1);

      // Finish original slow request and confirm replay
      releaseSlowHandler();
      const slowResp = await slowReqPromise;
      expect(slowResp.status).toBe(202);
      expect(await slowResp.json()).toEqual({ ok: true, call: 1 });

      await new Promise(r => setTimeout(r, 100));
      const replay = await fetch(url, {
        method:  'POST',
        headers: { 'x-idempotency-key': slowKey, 'x-dev-user-id': 'slow_lease_user' },
      });
      expect(replay.status).toBe(202);
      expect(await replay.json()).toEqual({ ok: true, call: 1 });
      expect(handlerCalls).toBe(1);

      // Simulate API crash while key is pending (no renewal, no completion)
      const crashKey = `crash-${uuidv4()}`;
      await redis.set(`idempotency:crash_user:${crashKey}`, `pending:${uuidv4()}`, 'NX', 'EX', 2);

      const immediateRetry = await fetch(url, {
        method:  'POST',
        headers: { 'x-idempotency-key': crashKey, 'x-dev-user-id': 'crash_user' },
      });
      expect(immediateRetry.status).toBe(409);

      // Wait past 2s crash expiry -> next retry succeeds
      await new Promise(r => setTimeout(r, 2200));
      const retryAfterCrash = fetch(url, {
        method:  'POST',
        headers: { 'x-idempotency-key': crashKey, 'x-dev-user-id': 'crash_user' },
      });
      await waitForCondition(() => typeof releaseSlowHandler === 'function' && handlerCalls === 2);
      releaseSlowHandler();
      const recoveredResp = await retryAfterCrash;
      expect(recoveredResp.status).toBe(202);
    } finally {
      await new Promise(resolve => server.close(resolve));
      getRedisClient().disconnect();
    }
  });

  test('8. Crash after reserve_stock.lua before HTTP response cache: retry returns original orderId, single decrement, single OrderReserved, user isolated', async () => {
    const item = await createTestItem({ totalStock: 2, priceCents: 500 });
    const sharedKey = `post-lua-crash-${uuidv4()}`;
    const originalOrderId = uuidv4();

    const { reserveStock }      = require('../src/services/redisService');
    const { COMPLETED_TTL_SEC } = require('../src/middleware/idempotency');

    const mappingKeyA   = `reservation:idempotency:crash_user_A:${sharedKey}`;
    const httpCacheKeyA = `idempotency:crash_user_A:${sharedKey}`;

    // 1. Simulate pending lease acquired, reserve_stock.lua succeeds, then API crashes
    //    before idempotency.js caches the 202 HTTP response, and the short lease expires.
    await redis.set(httpCacheKeyA, `pending:${uuidv4()}`, 'NX', 'EX', 1);
    const luaRes = await reserveStock({
      orderId:           originalOrderId,
      userId:            'crash_user_A',
      itemId:            item.id,
      amountCents:       500,
      idempotencyKey:    `crash_user_A:${sharedKey}`,
      reservationKey:    mappingKeyA,
      mappingTtlSeconds: COMPLETED_TTL_SEC,
    });
    expect(luaRes.status).toBe('SUCCESS');
    expect(await redis.get(`item:${item.id}:stock`)).toBe('1');

    // Wait for pending HTTP lease to expire (no cached HTTP response exists)
    await new Promise(r => setTimeout(r, 1200));
    expect(await redis.get(httpCacheKeyA)).toBeNull();

    // Verify durable reservation mapping TTL matches completed idempotency retention (86400s)
    const mappingTtl = await redis.ttl(mappingKeyA);
    expect(mappingTtl).toBeGreaterThan(86300);
    expect(mappingTtl).toBeLessThanOrEqual(COMPLETED_TTL_SEC);

    // 2. Retry the exact same request via HTTP against the live Docker stack
    const retry = await checkoutRequest({
      itemId:         item.id,
      userId:         'crash_user_A',
      idempotencyKey: sharedKey,
    });
    expect(retry.status).toBe(202);
    expect(retry.body.orderId).toBe(originalOrderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('1');

    // Verify only ONE OrderReserved event exists in orders:stream for crash_user_A
    const streamEntries = await redis.xrange('orders:stream', '-', '+');
    const parseStreamObj = (arr) => {
      const o = {};
      for (let i = 0; i < arr.length; i += 2) o[arr[i]] = arr[i + 1];
      return o;
    };
    const userAEvents = streamEntries
      .map(([, fields]) => parseStreamObj(fields))
      .filter(e => e.event === 'OrderReserved' && e.itemId === item.id && e.userId === 'crash_user_A');
    expect(userAEvents).toHaveLength(1);
    expect(userAEvents[0].orderId).toBe(originalOrderId);

    // 3. Different user with the exact same X-Idempotency-Key is isolated and reserves the remaining unit
    const userBResp = await checkoutRequest({
      itemId:         item.id,
      userId:         'crash_user_B',
      idempotencyKey: sharedKey,
    });
    expect(userBResp.status).toBe(202);
    expect(userBResp.body.orderId).not.toBe(originalOrderId);
    expect(await redis.get(`item:${item.id}:stock`)).toBe('0');

    const entriesAfterB = await redis.xrange('orders:stream', '-', '+');
    const userAEventsAfterB = entriesAfterB
      .map(([, fields]) => parseStreamObj(fields))
      .filter(e => e.event === 'OrderReserved' && e.itemId === item.id && e.userId === 'crash_user_A');
    const userBEventsAfterB = entriesAfterB
      .map(([, fields]) => parseStreamObj(fields))
      .filter(e => e.event === 'OrderReserved' && e.itemId === item.id && e.userId === 'crash_user_B');
    expect(userAEventsAfterB).toHaveLength(1);
    expect(userBEventsAfterB).toHaveLength(1);

    // 4. Even when stock is now 0, deleting HTTP cache & retrying crash_user_A still replays originalOrderId (not 409)
    await redis.del(httpCacheKeyA);
    const zeroStockRetry = await checkoutRequest({
      itemId:         item.id,
      userId:         'crash_user_A',
      idempotencyKey: sharedKey,
    });
    expect(zeroStockRetry.status).toBe(202);
    expect(zeroStockRetry.body.orderId).toBe(originalOrderId);
  });
});
