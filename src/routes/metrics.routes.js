/**
 * metrics.routes.js
 *
 * GET /api/metrics
 *
 * Returns a real-time snapshot of the system's health — no auth required
 * so it can be polled by monitoring tools or viewed in the browser.
 *
 * Fields:
 *   items        — per-item live stock straight from Redis (source of truth)
 *   pendingHolds — active reservations waiting for payment (ZCARD holds:expiry)
 *   dbOrderCount — total orders persisted in PostgreSQL (by status)
 *   streamLength — number of entries currently in orders:stream (XLEN)
 *   uptime       — process uptime in seconds
 */

const express          = require('express');
const router           = express.Router();
const { getRedisClient } = require('../config/redis');
const { query }          = require('../config/db');

router.get('/', async (_req, res) => {
  try {
    const redis = getRedisClient();

    // 1. All item stock keys in Redis
    const stockKeys   = await redis.keys('item:*:stock');
    const stockValues = stockKeys.length
      ? await redis.mget(...stockKeys)
      : [];

    const items = {};
    stockKeys.forEach((key, i) => {
      // key format: item:{uuid}:stock  → extract the uuid
      const itemId   = key.split(':')[1];
      items[itemId]  = parseInt(stockValues[i], 10);
    });

    // 2. Active holds waiting for payment
    const pendingHolds = await redis.zcard('holds:expiry');

    // 3. Order counts from PostgreSQL grouped by status
    const { rows: statusRows } = await query(
      `SELECT status, COUNT(*)::int AS count FROM orders GROUP BY status`
    );
    const dbOrders = {};
    let   dbOrderCount = 0;
    for (const row of statusRows) {
      dbOrders[row.status] = row.count;
      dbOrderCount        += row.count;
    }

    // 4. Stream backlog length
    const streamLength = await redis.xlen('orders:stream');

    return res.json({
      items,
      pendingHolds,
      dbOrderCount,
      dbOrders,
      streamLength,
      uptime: Math.floor(process.uptime()),
    });

  } catch (err) {
    console.error('[Metrics] Error:', err);
    return res.status(500).json({ error: 'Failed to fetch metrics' });
  }
});

module.exports = router;

