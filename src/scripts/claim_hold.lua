-- claim_hold.lua
-- Atomically verify hold ownership + expiry, consume hold, and remove from expiry sorted set.
-- KEYS[1] = hold:{orderId}
-- KEYS[2] = holds:expiry
-- ARGV[1] = callerUserId
-- ARGV[2] = nowMs (epoch ms)
-- ARGV[3] = orderId
-- Returns: { status, userId?, itemId? }

local val = redis.call('GET', KEYS[1])
if not val then
  return { 'NOT_FOUND' }
end

local userId, itemId, expireAtStr = string.match(val, '^(.+):([^:]+):(%d+)$')
if not userId then
  userId, itemId = string.match(val, '^(.+):([^:]+)$')
end

if userId ~= ARGV[1] then
  return { 'FORBIDDEN' }
end

local now = tonumber(ARGV[2])
local expireAt = expireAtStr and tonumber(expireAtStr) or nil
if expireAt and now > expireAt then
  return { 'EXPIRED' }
end

redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[3])

return { 'OK', userId, itemId }

