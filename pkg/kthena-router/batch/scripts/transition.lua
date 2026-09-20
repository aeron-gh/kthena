-- KEYS: job, lease, active. ARGV: expected, fence, next, timestampField, id, leaseValue, final, then field/value pairs.
-- Returns OK, CONFLICT_STATUS or CONFLICT_FENCE. A final transition drops the lease and the active entry.
if redis.call('HGET', KEYS[1], 'status') ~= ARGV[1] then
  return 'CONFLICT_STATUS'
end
if redis.call('HGET', KEYS[1], 'fence') ~= ARGV[2] then
  return 'CONFLICT_FENCE'
end
local fields = {'status', ARGV[3]}
if ARGV[4] ~= '' then
  fields[#fields + 1] = ARGV[4]
  fields[#fields + 1] = tonumber(redis.call('TIME')[1])
end
for i = 8, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('HSET', KEYS[1], unpack(fields))
if ARGV[7] == '1' then
  redis.call('ZREM', KEYS[3], ARGV[5])
  if redis.call('GET', KEYS[2]) == ARGV[6] then
    redis.call('DEL', KEYS[2])
  end
end
return 'OK'
