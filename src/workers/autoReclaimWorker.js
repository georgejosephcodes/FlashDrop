/**
 * autoReclaimWorker.js
 *
 * Polls the `holds:expiry` sorted set every 5 seconds.
 * Uses reclaim_hold.lua so hold deletion, sorted-set cleanup, stock restoration,
 * and OrderExpired stream append execute atomically in one Redis transaction.
 */

require('dotenv').config();
const { getRedisClient } = require('../config/redis');
const { reclaimHold }    = require('../services/redisService');

const POLL_INTERVAL_MS = 5000;

async function reclaimExpired(redis) {
  const now = Date.now();

  const expired = await redis.zrangebyscore('holds:expiry', 0, now);

  if (expired.length === 0) return;

  console.log(`[AutoReclaim] Found ${expired.length} expired hold(s)`);

  for (const orderId of expired) {
    try {
      const result = await reclaimHold(orderId);

      if (result.status !== 'RECLAIMED') {
        continue;
      }

      const { userId, itemId } = result;

      // Best-effort WebSocket push (durable OrderExpired is already in stream)
      redis.publish('order:events', JSON.stringify({
        type:   'order:expired',
        orderId,
        userId,
        reason: 'Payment timeout — hold released, stock restored.',
      })).catch((err) => {
        console.error(`[AutoReclaim] Pub/Sub notify failed for ${orderId}:`, err.message);
      });

      console.log(`[AutoReclaim] Reclaimed orderId=${orderId} itemId=${itemId} stock restored`);

    } catch (err) {
      console.error(`[AutoReclaim] Error processing orderId=${orderId}:`, err.message);
    }
  }
}

async function run() {
  const redis = getRedisClient();
  console.log(`[AutoReclaim] Starting — polling every ${POLL_INTERVAL_MS / 1000}s`);

  while (true) {
    try {
      await reclaimExpired(redis);
    } catch (err) {
      console.error('[AutoReclaim] Poll error:', err.message);
    }
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
  }
}

run().catch((err) => {
  console.error('[AutoReclaim] Fatal error:', err);
  process.exit(1);
});
