-- KEYS: job, checkpoint. ARGV: fence, then field/value pairs.
-- Only the current owner may record progress.
if redis.call('HGET', KEYS[1], 'fence') ~= ARGV[1] then
  return 'CONFLICT_FENCE'
end
local fields = {}
for i = 2, #ARGV do
  fields[#fields + 1] = ARGV[i]
end
redis.call('HSET', KEYS[2], unpack(fields))
return 'OK'
