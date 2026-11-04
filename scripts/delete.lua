-- ARGV[3] id.
-- Removes a job that no worker holds. Returns 'ok', 'not_found' or 'active'.
local id = ARGV[3]
local key = JOB_PREFIX .. id

local state = redis.call('HGET', key, 'state')
if not state then
  return 'not_found'
end
if state == 'active' then
  return 'active'
end

redis.call('ZREM', K_READY, id)
redis.call('ZREM', K_DELAYED, id)
redis.call('ZREM', K_DEAD, id)
redis.call('ZREM', K_COMPLETED, id)
redis.call('DEL', key)
bump('deleted')
return 'ok'
