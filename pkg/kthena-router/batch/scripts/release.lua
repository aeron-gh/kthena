-- KEYS: lease, queue, active, job. ARGV: leaseValue, id.
-- Hands an unfinished batch straight back to the queue instead of waiting for the lease
-- to expire. A batch somebody else now owns is left alone.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 'NOT_OURS'
end
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[3], ARGV[2])
local status = redis.call('HGET', KEYS[4], 'status')
if status == 'validating' or status == 'in_progress' or status == 'finalizing' or status == 'cancelling' then
  redis.call('ZADD', KEYS[2], 'NX', tonumber(redis.call('TIME')[1]), ARGV[2])
  return 'REQUEUED'
end
return 'RELEASED'
