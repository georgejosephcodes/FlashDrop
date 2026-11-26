/**
 * orderProcessorWorker.js
 *
 * Reads OrderReserved events from the Redis Stream using a Consumer Group.
 * For each event:
 *   1. Creates a hold key in Redis with a failsafe TTL > holdTTL so Redis TTL
 *      never deletes itemId before autoReclaimWorker can restore stock.
 *   2. Adds orderId to the holds:expiry sorted set (score = exact expiry deadline)
 *   3. Publishes to Redis Pub/Sub so Socket.io can push to the client
 *   4. ACKs the message so it leaves the Pending Entries List (PEL)
 */

require('dotenv').config();
const { getRedisClient } = require('../config/redis');
const env                = require('../config/env');

const STREAM_KEY  = 'orders:stream';
const GROUP_NAME  = 'orderProcessors';
const CONSUMER    = `worker-${process.pid}`;
const BLOCK_MS    = 2000;
const BATCH_COUNT = 10;

function parseFields(fields) {
  const obj = {};
  for (let i = 0; i < fields.length; i += 2) {
    obj[fields[i]] = fields[i + 1];
  }
  return obj;
}

async function initGroup(redis) {
  try {
    await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '0', 'MKSTREAM');
    console.log(`[OrderProcessor] Consumer group "${GROUP_NAME}" created`);
  } catch (err) {
    if (err.message.includes('BUSYGROUP')) {
      console.log(`[OrderProcessor] Consumer group "${GROUP_NAME}" already exists — OK`);
    } else {
      throw err;
    }
  }
}

async function processMessage(redis, streamId, fields) {
  const { event, orderId, userId, itemId } = fields;

  if (event !== 'OrderReserved') {
    await redis.xack(STREAM_KEY, GROUP_NAME, streamId);
    return;
  }

  const holdTTL     = env.HOLD_TTL_SECONDS;         // default 600s
  const expireAt    = Date.now() + holdTTL * 1000;  // exact deadline (epoch ms)
  const failsafeTTL = holdTTL + 3600;               // +1h buffer so reclaim always sees userId/itemId
  const holdKey     = `hold:${orderId}`;
  const holdValue   = `${userId}:${itemId}:${expireAt}`;

  // 1. Set hold key with failsafe TTL (> deadline)
  await redis.set(holdKey, holdValue, 'EX', failsafeTTL);

  // 2. Add to sorted set — score = exact expiry timestamp for reliable polling
  await redis.zadd('holds:expiry', expireAt, orderId);

  // 3. Notify WebSocket gateway via Pub/Sub
  await redis.publish('order:events', JSON.stringify({
    type:      'order:reserved',
    orderId,
    userId,
    itemId,
    expiresAt: new Date(expireAt).toISOString(),
  }));

  // 4. ACK — removes from PEL, message is fully processed
  await redis.xack(STREAM_KEY, GROUP_NAME, streamId);

  console.log(`[OrderProcessor] Reserved orderId=${orderId} userId=${userId} expires=${new Date(expireAt).toISOString()}`);
}

async function drainPending(redis) {
  while (true) {
    const pending = await redis.xpending(STREAM_KEY, GROUP_NAME, '-', '+', BATCH_COUNT);
    if (!pending || pending.length === 0) break;

    console.log(`[OrderProcessor] Reclaiming ${pending.length} pending message(s) from PEL`);
    let progressed = 0;

    for (const p of pending) {
      const claimed = await redis.xclaim(STREAM_KEY, GROUP_NAME, CONSUMER, 0, p[0]);
      if (claimed && claimed.length > 0 && claimed[0][1]) {
        const [streamId, fields] = claimed[0];
        await processMessage(redis, streamId, parseFields(fields));
        progressed++;
      } else {
        // Entry no longer in stream — ACK so PEL clears cleanly
        await redis.xack(STREAM_KEY, GROUP_NAME, p[0]);
        progressed++;
      }
    }

    if (progressed === 0) break;
  }
}

async function run() {
  const redis = getRedisClient();
  await initGroup(redis);

  console.log(`[OrderProcessor] Starting consumer "${CONSUMER}" on stream "${STREAM_KEY}"`);

  // Recover ALL pending messages stuck in PEL from previous crashed workers
  await drainPending(redis);

  while (true) {
    try {
      const results = await redis.xreadgroup(
        'GROUP', GROUP_NAME, CONSUMER,
        'COUNT', BATCH_COUNT,
        'BLOCK', BLOCK_MS,
        'STREAMS', STREAM_KEY, '>'
      );

      if (!results) continue;

      for (const [, messages] of results) {
        for (const [streamId, fields] of messages) {
          await processMessage(redis, streamId, parseFields(fields));
        }
      }
    } catch (err) {
      console.error('[OrderProcessor] Error:', err.message);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

run().catch((err) => {
  console.error('[OrderProcessor] Fatal error:', err);
  process.exit(1);
});
