-- ARGV[3] id, ARGV[4] delivery token.
-- Hands a job back during shutdown. The attempt is refunded because the job did
-- not fail, so rolling restarts do not push jobs toward the dead letter queue.
-- Returns 'ok' or 'lost'.
local id, token = ARGV[3], ARGV[4]
local key = JOB_PREFIX .. id

if held_state(key, token) ~= 'active' then
  return 'lost'
end

redis.call('ZREM', K_ACTIVE, id)
redis.call('HINCRBY', key, 'attempts', -1)
redis.call('HDEL', key, 'lease_owner', 'lease_until')
make_ready(id, key)
bump('released')
wake()
return 'ok'
