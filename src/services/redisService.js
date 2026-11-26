const fs                = require('fs');
const path              = require('path');
const { getRedisClient } = require('../config/redis');

const STREAM_KEY = 'orders:stream';
const _shas      = {};  // cached EVALSHA handles by script filename

async function loadScript(filename) {
  if (_shas[filename]) return _shas[filename];

  const redis  = getRedisClient();
  const script = fs.readFileSync(
    path.join(__dirname, '../scripts', filename),
    'utf8'
  );

  _shas[filename] = await redis.script('LOAD', script);
  console.log(`[Redis] Lua script ${filename} loaded (SHA: ${_shas[filename]})`);
  return _shas[filename];
}

/**
 * Execute a cached Lua script via EVALSHA, reloading automatically if Redis
 * flushed its script cache (NOSCRIPT error).
 */
async function evalScript(filename, numKeys, ...args) {
  const redis = getRedisClient();
  let sha     = await loadScript(filename);
  try {
    return await redis.evalsha(sha, numKeys, ...args);
  } catch (err) {
    if (err.message && err.message.includes('NOSCRIPT')) {
      delete _shas[filename];
      sha = await loadScript(filename);
      return await redis.evalsha(sha, numKeys, ...args);
    }
    throw err;
  }
}

/**
 * Load all Lua scripts into Redis on startup using SCRIPT LOAD.
 */
async function loadLuaScript() {
  await loadScript('reserve_stock.lua');
  await loadScript('claim_hold.lua');
  await loadScript('reclaim_hold.lua');
  return _shas['reserve_stock.lua'];
}

/**
 * Atomically check durable reservation mapping, decrement stock, append
 * OrderReserved to orders:stream, and persist user-scoped reservation mapping.
 *
 * @param {{ orderId: string, userId: string, itemId: string, amountCents: number, idempotencyKey: string, reservationKey: string, mappingTtlSeconds?: number }} params
 * @returns {Promise<{ status: 'SUCCESS'|'EXISTING'|'SOLD_OUT', orderId?: string, streamId?: string }>}
 */
async function reserveStock({
  orderId,
  userId,
  itemId,
  amountCents,
  idempotencyKey,
  reservationKey,
  mappingTtlSeconds = 86400,
}) {
  const res = await evalScript(
    'reserve_stock.lua',
    3,
    `item:${itemId}:stock`,
    STREAM_KEY,
    reservationKey,
    orderId,
    userId,
    itemId,
    String(amountCents),
    idempotencyKey,
    String(Date.now()),
    String(mappingTtlSeconds)
  );

  if (res[0] === 2) {
    return { status: 'EXISTING', orderId: res[1] };
  }
  if (res[0] === 1) {
    return { status: 'SUCCESS', streamId: res[1], orderId: res[2] };
  }
  return { status: 'SOLD_OUT' };
}

/**
 * Atomically claim an active hold for payment, verify ownership & deadline,
 * consume the hold, and append OrderConfirmed to orders:stream.
 */
async function claimHold(orderId, callerUserId) {
  const [status, userId, itemId, streamId] = await evalScript(
    'claim_hold.lua',
    3,
    `hold:${orderId}`,
    'holds:expiry',
    STREAM_KEY,
    callerUserId,
    String(Date.now()),
    orderId
  );
  return { status, userId, itemId, streamId };
}

/**
 * Atomically reclaim an expired hold, restore stock, and append OrderExpired
 * to orders:stream.
 */
async function reclaimHold(orderId) {
  const [status, userId, itemId, streamId] = await evalScript(
    'reclaim_hold.lua',
    3,
    `hold:${orderId}`,
    'holds:expiry',
    STREAM_KEY,
    orderId,
    String(Date.now())
  );
  return { status, userId, itemId, streamId };
}

/**
 * Seed stock (and optional cached metadata) for a new item into Redis.
 */
async function seedItemStock(itemId, stock, meta = null) {
  const redis = getRedisClient();
  await redis.set(`item:${itemId}:stock`, stock);
  if (meta) {
    await redis.set(`item:${itemId}:meta`, JSON.stringify(meta));
  }
  console.log(`[Redis] Seeded item:${itemId}:stock = ${stock}`);
}

/**
 * Get cached item metadata (priceCents, saleStartAt) from Redis.
 */
async function getItemMeta(itemId) {
  const redis = getRedisClient();
  const raw   = await redis.get(`item:${itemId}:meta`);
  return raw ? JSON.parse(raw) : null;
}

/**
 * Get current live stock for an item from Redis.
 */
async function getLiveStock(itemId) {
  const redis = getRedisClient();
  const val   = await redis.get(`item:${itemId}:stock`);
  return val === null ? null : parseInt(val, 10);
}

/**
 * Restore stock by 1.
 */
async function restoreStock(itemId) {
  const redis = getRedisClient();
  return redis.incr(`item:${itemId}:stock`);
}

module.exports = {
  loadLuaScript,
  reserveStock,
  claimHold,
  reclaimHold,
  seedItemStock,
  getItemMeta,
  getLiveStock,
  restoreStock,
};
