-- reserve_stock.lua
-- Atomic stock check + decrement. Runs as a single Redis transaction.
-- KEYS[1] = item:{itemId}:stock
-- Returns: 0 = SOLD_OUT, 1 = SUCCESS

local stock = tonumber(redis.call('GET', KEYS[1]))

if not stock or stock <= 0 then
  return 0  -- SOLD_OUT
end

redis.call('DECR', KEYS[1])
return 1  -- SUCCESS

