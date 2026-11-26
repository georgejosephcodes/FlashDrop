const fs                = require('fs');
const path              = require('path');
const { getRedisClient } = require('../config/redis');

const _shas = {};  // cached EVALSHA handles by script filename

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
 * Atomically reserve 1 unit of stock for an item.
 * @param {string} itemId
 * @returns {Promise<'SUCCESS'|'SOLD_OUT'>}
 */
async function reserveStock(itemId) {
  const key    = `item:${itemId}:stock`;
  const result = await evalScript('reserve_stock.lua', 1, key);
  return result === 1 ? 'SUCCESS' : 'SOLD_OUT';
}

/**
 * Atomically claim an active hold for payment after verifying ownership & deadline.
 * @param {string} orderId
 * @param {string} callerUserId
 * @returns {Promise<{ status: 'OK'|'NOT_FOUND'|'FORBIDDEN'|'EXPIRED', userId?: string, itemId?: string }>}
 */
async function claimHold(orderId, callerUserId) {
  const [status, userId, itemId] = await evalScript(
    'claim_hold.lua',
    2,
    `hold:${orderId}`,
    'holds:expiry',
    callerUserId,
    String(Date.now()),
    orderId
  );
  return { status, userId, itemId };
}

/**
 * Atomically reclaim an expired hold and restore 1 unit of stock.
 * @param {string} orderId
 * @returns {Promise<{ status: 'RECLAIMED'|'ALREADY_HANDLED'|'NOT_EXPIRED', userId?: string, itemId?: string }>}
 */
async function reclaimHold(orderId) {
  const [status, userId, itemId] = await evalScript(
    'reclaim_hold.lua',
    2,
    `hold:${orderId}`,
    'holds:expiry',
    orderId,
    String(Date.now())
  );
  return { status, userId, itemId };
}

/**
 * Seed stock for a new item into Redis.
 * Called by the items controller after inserting to Postgres.
 * @param {string} itemId
 * @param {number} stock
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
 * @param {string} itemId
 * @returns {Promise<{ priceCents: number, saleStartAt: string }|null>}
 */
async function getItemMeta(itemId) {
  const redis = getRedisClient();
  const raw   = await redis.get(`item:${itemId}:meta`);
  return raw ? JSON.parse(raw) : null;
}

/**
 * Get current live stock for an item from Redis.
 * @param {string} itemId
 * @returns {Promise<number>}
 */
async function getLiveStock(itemId) {
  const redis = getRedisClient();
  const val   = await redis.get(`item:${itemId}:stock`);
  return val === null ? null : parseInt(val, 10);
}

/**
 * Restore stock by 1 (used as compensating action on publish failure).
 * @param {string} itemId
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
