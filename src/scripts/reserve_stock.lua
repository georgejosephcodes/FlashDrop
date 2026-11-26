-- reserve_stock.lua
-- Atomic idempotency reservation lookup, stock check/decrement, OrderReserved
-- stream publish, and durable user-scoped reservation mapping in one transaction.
-- KEYS[1] = item:{itemId}:stock
-- KEYS[2] = orders:stream
-- KEYS[3] = reservation:idempotency:{userId}:{rawKey}
-- ARGV[1] = orderId
-- ARGV[2] = userId
-- ARGV[3] = itemId
-- ARGV[4] = amountCents
-- ARGV[5] = idempotencyKey
-- ARGV[6] = timestamp (epoch ms)
-- ARGV[7] = mappingTtlSeconds (matches completed idempotency retention)
-- Returns:
--   { 2, existingOrderId }      = EXISTING (idempotent replay from durable mapping)
--   { 0 }                       = SOLD_OUT
--   { 1, streamId, orderId }    = SUCCESS

local existingOrderId = redis.call('GET', KEYS[3])
if existingOrderId then
  return { 2, existingOrderId }
end

local stock = tonumber(redis.call('GET', KEYS[1]))

if not stock or stock <= 0 then
  return { 0 }
end

redis.call('DECR', KEYS[1])

local streamId = redis.call(
  'XADD', KEYS[2], '*',
  'event',          'OrderReserved',
  'orderId',        ARGV[1],
  'userId',         ARGV[2],
  'itemId',         ARGV[3],
  'amountCents',    ARGV[4],
  'idempotencyKey', ARGV[5],
  'timestamp',      ARGV[6]
)

redis.call('SET', KEYS[3], ARGV[1], 'EX', tonumber(ARGV[7]))

return { 1, streamId, ARGV[1] }
