-- KEYS: job, checkpoint. ARGV: fence, total, completed, failed, then checkpoint field/value pairs.
-- Only the current owner may record progress, and the counts stay visible on the batch itself.
if redis.call('HGET', KEYS[1], 'fence') ~= ARGV[1] then
  return 'CONFLICT_FENCE'
end
redis.call('HSET', KEYS[1], 'total', ARGV[2], 'completed', ARGV[3], 'failed', ARGV[4])
local fields = {}
for i = 5, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('HSET', KEYS[2], unpack(fields))
return 'OK'
