/**
 * orderProcessorWorker.js
 *
 * Reads OrderReserved events from the Redis Stream using a Consumer Group.
 * For each event:
 *   1. Creates a 10-minute hold key in Redis (SET hold:{orderId})
 *   2. Adds orderId to the sorted set for reliable expiry polling
 *   3. Publishes to Redis Pub/Sub so the WebSocket gateway can push to the client
 *   4. ACKs the message so it leaves the Pending Entries List (PEL)
 *
 * If this worker crashes mid-message the entry stays unACK'd in the PEL
 * and will be reclaimed on restart — zero lost events.
 */

require('dotenv').config();
const { getRedisClient }  = require('../config/redis');
const env                 = require('../config/env');

const STREAM_KEY   = 'orders:stream';
const GROUP_NAME   = 'orderProcessors';
const CONSUMER     = `worker-${process.pid}`;
const BLOCK_MS     = 2000;   // block up to 2s waiting for new entries
const BATCH_COUNT  = 10;     // messages per read

/** Parse a flat Redis stream field array into a plain object */
function parseFields(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

async function processMessage(redis, streamId, fields) {
  const { event, orderId, userId, itemId } = fields;

  if (event !== 'OrderReserved') {
    // Skip events not meant for this worker; still ACK so they don't clog PEL
    await redis.xack(STREAM_KEY, GROUP_NAME, streamId);
    return;
  }

  const holdTTL      = env.HOLD_TTL_SECONDS;                       // default 600
  const expireAt     = Date.now() + holdTTL * 1000;                // epoch ms
  const holdKey      = `hold:${orderId}`;
  const holdValue    = `${userId}:${itemId}`;

  // 1. Set the reservation hold with TTL (failsafe if worker dies before ZADD)
  await redis.set(holdKey, holdValue, 'EX', holdTTL);

  // 2. Add to sorted set — score = expiry timestamp for reliable polling
  //    autoReclaimWorker polls this set every 5s instead of relying on keyspace events
  await redis.zadd('holds:expiry', expireAt, orderId);

  // 3. Notify WebSocket gateway via Pub/Sub
  //    socketService subscribes to this channel and emits to room:user_{userId}
  await redis.publish('order:events', JSON.stringify({
    type:    'order:reserved',
    orderId,
    userId,
    itemId,
    expiresAt: new Date(expireAt).toISOString(),
  }));

  // 4. ACK — removes from PEL, message is fully processed
  await redis.xack(STREAM_KEY, GROUP_NAME, streamId);

  console.log(`[OrderProcessor] Reserved orderId=${orderId} userId=${userId} expires=${new Date(expireAt).toISOString()}`);
}

async function run() {
  // Worker needs its own Redis client for XREADGROUP (blocking command)
  const redis = getRedisClient();

  console.log(`[OrderProcessor] Starting consumer "${CONSUMER}" on stream "${STREAM_KEY}"`);

  // On startup: reclaim any messages stuck in PEL from a previous crashed worker
  const pending = await redis.xpending(STREAM_KEY, GROUP_NAME, '-', '+', 10);
  if (pending.length > 0) {
    console.log(`[OrderProcessor] Reclaiming ${pending.length} pending messages from PEL`);
    for (const p of pending) {
      const claimed = await redis.xclaim(STREAM_KEY, GROUP_NAME, CONSUMER, 0, p[0]);
      if (claimed.length > 0) {
        const [streamId, fields] = claimed[0];
        await processMessage(redis, streamId, parseFields(fields));
      }
    }
  }

  // Main loop: block-read new messages
  while (true) {
    try {
      const results = await redis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER,
        'COUNT', BATCH_COUNT,
        'BLOCK', BLOCK_MS,
        'STREAMS', STREAM_KEY, '>'
      );

      if (!results) continue;  // timeout — no new messages, loop again

      for (const [, messages] of results) {
        for (const [streamId, fields] of messages) {
          await processMessage(redis, streamId, parseFields(fields));
        }
      }
    } catch (err) {
      console.error('[OrderProcessor] Error:', err.message);
      await new Promise(r => setTimeout(r, 1000));  // back off before retry
    }
  }
}

run();

