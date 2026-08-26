-- Flash-sale readiness pass, Option C: Redis is the hot-path authority for
-- both the idempotency check AND the stock decrement/increment -- both
-- happen in one atomic script execution, so there's no gap between "check"
-- and "mutate" for a race to land in, the same guarantee the Postgres
-- WHERE clause gave before, just enforced by Redis instead.
--
-- Every successful mutation is queued onto a Redis Stream (XADD) in the
-- SAME atomic script run -- that's what makes the eventual Postgres
-- reconciliation durable-in-intent even though it happens later: the
-- "this needs to be reconciled" fact is recorded atomically with the
-- mutation itself, not as a separate step that could be skipped.
--
-- KEYS[1] = idempotency key   (idempotency:inventory:{op}:{idempotencyKey})
-- KEYS[2] = stock key         (stock:{sku})
-- KEYS[3] = reconciliation stream key (stock:reconciliation)
-- ARGV[1] = quantity
-- ARGV[2] = sku
-- ARGV[3] = idempotency TTL, seconds
-- ARGV[4] = operation: "RESERVE" or "RELEASE"

local cached = redis.call('GET', KEYS[1])
if cached then
  return cached
end

local qty = tonumber(ARGV[1])
local current = redis.call('GET', KEYS[2])
if not current then
  return 'ERROR:SKU_NOT_FOUND'
end
current = tonumber(current)

local result
if ARGV[4] == 'RESERVE' then
  if current >= qty then
    redis.call('DECRBY', KEYS[2], qty)
    redis.call('XADD', KEYS[3], '*', 'sku', ARGV[2], 'qty', ARGV[1], 'op', 'RESERVE')
    result = 'OK:' .. (current - qty)
  else
    result = 'FAIL:' .. current
  end
else
  redis.call('INCRBY', KEYS[2], qty)
  redis.call('XADD', KEYS[3], '*', 'sku', ARGV[2], 'qty', ARGV[1], 'op', 'RELEASE')
  result = 'OK:' .. (current + qty)
end

redis.call('SET', KEYS[1], result, 'EX', ARGV[3])
return result
