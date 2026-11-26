-- reclaim_hold.lua
-- Atomically verify hold expiry, delete hold, remove from sorted set, and restore stock.
-- KEYS[1] = hold:{orderId}
-- KEYS[2] = holds:expiry
-- ARGV[1] = orderId
-- ARGV[2] = nowMs (epoch ms)
-- Returns: { status, userId?, itemId? }

local val = redis.call('GET', KEYS[1])
if not val then
  redis.call('ZREM', KEYS[2], ARGV[1])
  return { 'ALREADY_HANDLED' }
end

local userId, itemId, expireAtStr = string.match(val, '^(.+):([^:]+):(%d+)$')
if not userId then
  userId, itemId = string.match(val, '^(.+):([^:]+)$')
end

local now = tonumber(ARGV[2])
local expireAt = expireAtStr and tonumber(expireAtStr) or nil
if expireAt and now < expireAt then
  return { 'NOT_EXPIRED' }
end

local deleted = redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])

if deleted == 1 and itemId then
  redis.call('INCR', 'item:' .. itemId .. ':stock')
  return { 'RECLAIMED', userId, itemId }
end

return { 'ALREADY_HANDLED' }

