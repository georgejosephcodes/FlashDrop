/**
 * autoReclaimWorker.js
 *
 * Polls the `holds:expiry` sorted set every 5 seconds.
 * Any orderId whose score (expiry timestamp in ms) is <= now has timed out
 * without payment — this worker restores its inventory and fires the
 * OrderExpired event.
 *
 * WHY a sorted set instead of Redis keyspace notifications:
 *   Keyspace notifications can be dropped on reconnect or when the subscriber
 *   is restarting. Polling a sorted set is reliable — entries stay until
 *   explicitly removed, so a crashed worker will catch them on restart.
 */

require('dotenv').config();
const { getRedisClient }     = require('../config/redis');
const { restoreStock }       = require('../services/redisService');
const { publishOrderExpired } = require('../services/streamService');

const POLL_INTERVAL_MS = 5000;

async function reclaimExpired(redis) {
  const now = Date.now();

  // Get all entries with score (expiry timestamp) <= now
  const expired = await redis.zrangebyscore('holds:expiry', 0, now);

  if (expired.length === 0) return;

  console.log(`[AutoReclaim] Found ${expired.length} expired hold(s)`);

  for (const orderId of expired) {
    try {
      const holdKey = `hold:${orderId}`;
      const holdVal = await redis.get(holdKey);

      if (!holdVal) {
        // Hold already cleared (user paid in time) — just clean up sorted set
        await redis.zrem('holds:expiry', orderId);
        continue;
      }

      // holdVal format: "{userId}:{itemId}"
      const [userId, itemId] = holdVal.split(':');

      // 1. Restore 1 unit of stock back to Redis — next buyer can grab it
      await restoreStock(itemId);

      // 2. Remove from sorted set
      await redis.zrem('holds:expiry', orderId);

      // 3. Delete the hold key (may already be expired via TTL, DEL is safe)
      await redis.del(holdKey);

      // 4. Publish OrderExpired event to stream → dbSyncWorker will persist it
      await publishOrderExpired({ orderId, userId, itemId });

      // 5. Notify client via Pub/Sub → socketService pushes order:expired
      await redis.publish('order:events', JSON.stringify({
        type:    'order:expired',
        orderId,
        userId,
        reason:  'Payment timeout — hold released, stock restored.',
      }));

      console.log(`[AutoReclaim] Reclaimed orderId=${orderId} itemId=${itemId} stock restored`);

    } catch (err) {
      console.error(`[AutoReclaim] Error processing orderId=${orderId}:`, err.message);
      // Don't remove from sorted set on error — will retry next poll cycle
    }
  }
}

async function run() {
  const redis = getRedisClient();
  console.log(`[AutoReclaim] Starting — polling every ${POLL_INTERVAL_MS / 1000}s`);

  // Run immediately on start, then on interval
  while (true) {
    await reclaimExpired(redis);
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));
  }
}

run().catch((err) => {
  console.error('[AutoReclaim] Fatal error:', err);
  process.exit(1);
});

