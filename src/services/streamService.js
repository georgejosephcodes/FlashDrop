const { getRedisClient } = require('../config/redis');

const STREAM_KEY    = 'orders:stream';
const GROUP_NAME    = 'orderProcessors';

/**
 * Create the consumer group on startup if it doesn't already exist.
 * Uses $ so the group only reads NEW messages (not backlog).
 */
async function initStream() {
  const redis = getRedisClient();
  try {
    await redis.xgroup('CREATE', STREAM_KEY, GROUP_NAME, '$', 'MKSTREAM');
    console.log(`[Stream] Consumer group "${GROUP_NAME}" created on "${STREAM_KEY}"`);
  } catch (err) {
    if (err.message.includes('BUSYGROUP')) {
      console.log(`[Stream] Consumer group "${GROUP_NAME}" already exists — OK`);
    } else {
      throw err;
    }
  }
}

/**
 * Publish an OrderReserved event to the Redis Stream.
 * @param {{ orderId, userId, itemId, amountCents, idempotencyKey }} payload
 * @returns {Promise<string>} stream entry ID
 */
async function publishOrderReserved({ orderId, userId, itemId, amountCents, idempotencyKey }) {
  const redis = getRedisClient();
  const id    = await redis.xadd(
    STREAM_KEY, '*',
    'event',          'OrderReserved',
    'orderId',        orderId,
    'userId',         userId,
    'itemId',         itemId,
    'amountCents',    String(amountCents),
    'idempotencyKey', idempotencyKey,
    'timestamp',      Date.now().toString()
  );
  console.log(`[Stream] OrderReserved published → entry ${id}`);
  return id;
}

/**
 * Publish an OrderConfirmed event.
 */
async function publishOrderConfirmed({ orderId, userId, itemId }) {
  const redis = getRedisClient();
  return redis.xadd(
    STREAM_KEY, '*',
    'event',   'OrderConfirmed',
    'orderId', orderId,
    'userId',  userId,
    'itemId',  itemId,
    'timestamp', Date.now().toString()
  );
}

/**
 * Publish an OrderExpired event (from auto-reclaim worker).
 */
async function publishOrderExpired({ orderId, userId, itemId }) {
  const redis = getRedisClient();
  return redis.xadd(
    STREAM_KEY, '*',
    'event',   'OrderExpired',
    'orderId', orderId,
    'userId',  userId,
    'itemId',  itemId,
    'timestamp', Date.now().toString()
  );
}

module.exports = { initStream, publishOrderReserved, publishOrderConfirmed, publishOrderExpired, STREAM_KEY, GROUP_NAME };

