-- ARGV[3] id, ARGV[4] delivery token, ARGV[5] error text, ARGV[6] retry delay_ms,
-- ARGV[7] '1' when the failure is permanent.
-- Returns 'retry', 'dead' or 'lost'.
local id, token, message = ARGV[3], ARGV[4], ARGV[5]
local delay_ms, permanent = tonumber(ARGV[6]), ARGV[7] == '1'
local key = JOB_PREFIX .. id

if held_state(key, token) ~= 'active' then
  return 'lost'
end

local now = now_ms()
local values = redis.call('HMGET', key, 'attempts', 'max_attempts')
redis.call('ZREM', K_ACTIVE, id)
redis.call('HSET', key, 'last_error', message)
redis.call('HDEL', key, 'lease_owner', 'lease_until')
bump('failed')

if permanent or tonumber(values[1]) >= tonumber(values[2]) then
  make_dead(id, key, now)
  return 'dead'
end

local run_at = now + delay_ms
redis.call('ZADD', K_DELAYED, num(run_at), id)
redis.call('HSET', key, 'state', 'delayed', 'run_at', num(run_at))
bump('retried')
return 'retry'
