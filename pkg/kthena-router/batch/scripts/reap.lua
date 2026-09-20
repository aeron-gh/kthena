-- KEYS: active, queue. ARGV: prefix, limit.
-- Requeues every claimed job whose lease is gone, and forgets the ones that already finished.
local now = tonumber(redis.call('TIME')[1])
local prefix = ARGV[1]
local requeued = 0
for _, id in ipairs(redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1)) do
  if redis.call('EXISTS', prefix .. 'lease:' .. id) == 0 then
    local status = redis.call('HGET', prefix .. 'job:' .. id, 'status')
    if status == 'validating' or status == 'in_progress' or status == 'finalizing' or status == 'cancelling' then
      requeued = requeued + redis.call('ZADD', KEYS[2], 'NX', now, id)
    else
      redis.call('ZREM', KEYS[1], id)
    end
  end
end
return requeued
