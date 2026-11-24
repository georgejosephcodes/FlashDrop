const fs                = require('fs');
const path              = require('path');
const { getRedisClient } = require('../config/redis');

let _sha = null;  // cached EVALSHA handle

/**
 * Load reserve_stock.lua into Redis on startup using SCRIPT LOAD.
 * Returns the SHA1 hash used for EVALSHA calls.
 */
async function loadLuaScript() {
  if (_sha) return _sha;

  const redis  = getRedisClient();
  const script = fs.readFileSync(
    path.join(__dirname, '../scripts/reserve_stock.lua'),
    'utf8'
  );

  _sha = await redis.script('LOAD', script);
  console.log(`[Redis] Lua script loaded (SHA: ${_sha})`);
  return _sha;
}

/**
 * Atomically reserve 1 unit of stock for an item.
 * @param {string} itemId
 * @returns {Promise<'SUCCESS'|'SOLD_OUT'>}
 */
async function reserveStock(itemId) {
  const redis = getRedisClient();
  const sha   = await loadLuaScript();
  const key   = `item:${itemId}:stock`;

  const result = await redis.evalsha(sha, 1, key);
  return result === 1 ? 'SUCCESS' : 'SOLD_OUT';
}

/**
 * Seed stock for a new item into Redis.
 * Called by the items controller after inserting to Postgres.
 * @param {string} itemId
 * @param {number} stock
 */
async function seedItemStock(itemId, stock) {
  const redis = getRedisClient();
  await redis.set(`item:${itemId}:stock`, stock);
  console.log(`[Redis] Seeded item:${itemId}:stock = ${stock}`);
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
 * Restore stock by 1 (used by auto-reclaim worker on hold expiry).
 * @param {string} itemId
 */
async function restoreStock(itemId) {
  const redis = getRedisClient();
  return redis.incr(`item:${itemId}:stock`);
}

module.exports = { loadLuaScript, reserveStock, seedItemStock, getLiveStock, restoreStock };

