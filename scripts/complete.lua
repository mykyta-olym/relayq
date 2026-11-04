-- ARGV[3] id, ARGV[4] delivery token, ARGV[5] retention_ms.
-- Returns 'ok' or 'lost'.
local id, token, retention = ARGV[3], ARGV[4], tonumber(ARGV[5])
local key = JOB_PREFIX .. id

local state = held_state(key, token)
if state == 'completed' then
  return 'ok' -- the same ack sent twice after a lost reply
end
if state ~= 'active' then
  return 'lost'
end

local now = now_ms()
redis.call('ZREM', K_ACTIVE, id)
bump('completed')

if retention > 0 then
  redis.call('HSET', key, 'state', 'completed', 'completed_at', num(now))
  redis.call('HDEL', key, 'lease_owner', 'lease_until')
  redis.call('ZADD', K_COMPLETED, num(now), id)
  redis.call('PEXPIRE', key, retention)
else
  redis.call('DEL', key)
end
return 'ok'
