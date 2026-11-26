-- claim_hold.lua
-- Atomically verify hold ownership + expiry, consume hold, clean expiry set,
-- and append OrderConfirmed to orders:stream.
-- KEYS[1] = hold:{orderId}
-- KEYS[2] = holds:expiry
-- KEYS[3] = orders:stream
-- ARGV[1] = callerUserId
-- ARGV[2] = nowMs (epoch ms)
-- ARGV[3] = orderId
-- Returns: { status, userId?, itemId?, streamId? }

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

local streamId = redis.call(
  'XADD', KEYS[3], '*',
  'event',     'OrderConfirmed',
  'orderId',   ARGV[3],
  'userId',    userId,
  'itemId',    itemId,
  'timestamp', ARGV[2]
)

return { 'OK', userId, itemId, streamId }
