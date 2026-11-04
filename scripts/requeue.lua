-- ARGV[3] id, or an empty string to take the oldest dead jobs.
-- ARGV[4] batch limit when ARGV[3] is empty.
-- Moves dead jobs back to ready with a fresh attempt budget. The delivery counter
-- keeps rising so tokens from earlier deliveries stay invalid.
-- Returns the number of jobs moved.
local single, limit = ARGV[3], tonumber(ARGV[4])

local ids
if single ~= '' then
  if not redis.call('ZSCORE', K_DEAD, single) then
    return 0
  end
  ids = { single }
else
  ids = redis.call('ZRANGE', K_DEAD, 0, limit - 1)
end

local moved = 0
for _, id in ipairs(ids) do
  redis.call('ZREM', K_DEAD, id)
  local key = JOB_PREFIX .. id
  if redis.call('EXISTS', key) == 1 then
    redis.call('HSET', key, 'attempts', 0)
    redis.call('HDEL', key, 'failed_at')
    make_ready(id, key)
    bump('requeued')
    moved = moved + 1
  end
end

if moved > 0 then
  wake()
end
return moved
