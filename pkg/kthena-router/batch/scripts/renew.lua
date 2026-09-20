-- KEYS: lease, job. ARGV: leaseValue, leaseMs.
-- Returns the current job status, or LOST when the lease is no longer ours.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
  return 'LOST'
end
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
return redis.call('HGET', KEYS[2], 'status')
