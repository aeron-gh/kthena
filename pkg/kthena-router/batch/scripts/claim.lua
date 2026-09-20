-- KEYS: queue, fence, active. ARGV: holder, leaseMs, prefix, maxScan.
-- Returns {id, fence, status, overdue} for the claimed job, or false when none is claimable.
local now = tonumber(redis.call('TIME')[1])
local prefix = ARGV[3]
for _ = 1, tonumber(ARGV[4]) do
  local item = redis.call('ZPOPMIN', KEYS[1])
  if #item == 0 then
    return false
  end
  local id = item[1]
  local job = prefix .. 'job:' .. id
  local status = redis.call('HGET', job, 'status')
  local expiresAt = tonumber(redis.call('HGET', job, 'expires_at') or '0')
  local overdue = expiresAt > 0 and now > expiresAt
  if status == 'validating' and overdue then
    redis.call('HSET', job, 'status', 'expired', 'expired_at', now)
    redis.call('ZREM', KEYS[3], id)
  elseif status == 'validating' or status == 'in_progress' or status == 'finalizing' or status == 'cancelling' then
    local fence = redis.call('INCR', KEYS[2])
    redis.call('SET', prefix .. 'lease:' .. id, ARGV[1] .. ':' .. fence, 'PX', tonumber(ARGV[2]))
    redis.call('HSET', job, 'holder', ARGV[1], 'fence', fence)
    redis.call('ZADD', KEYS[3], now, id)
    local od = '0'
    if overdue and status == 'in_progress' then
      od = '1'
    end
    return {id, tostring(fence), status, od}
  else
    redis.call('ZREM', KEYS[3], id)
  end
end
return false
