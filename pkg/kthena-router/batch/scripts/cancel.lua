-- KEYS: job, queue, lease, active. ARGV: id.
-- Cancels a queued job outright, asks a running one to stop, and leaves anything else alone.
local now = tonumber(redis.call('TIME')[1])
local status = redis.call('HGET', KEYS[1], 'status')
if not status then
  return 'NOT_FOUND'
end
if status == 'validating' and redis.call('EXISTS', KEYS[3]) == 0 then
  redis.call('ZREM', KEYS[2], ARGV[1])
  redis.call('ZREM', KEYS[4], ARGV[1])
  redis.call('HSET', KEYS[1], 'status', 'cancelled', 'cancelled_at', now)
  return 'cancelled'
end
if status == 'validating' or status == 'in_progress' then
  redis.call('HSET', KEYS[1], 'status', 'cancelling', 'cancelling_at', now)
  return 'cancelling'
end
return status
