-- ARGV[3] batch limit, ARGV[4] completed retention_ms.
-- One pass of the periodic work. Any number of workers may run it at once: each
-- step is a set move inside one script, so a job is only moved by one of them.
-- Returns {promoted, reclaimed, buried, trimmed}.
local limit, retention = tonumber(ARGV[3]), tonumber(ARGV[4])
local now = now_ms()
local promoted, reclaimed, buried = 0, 0, 0

-- Delayed, scheduled and retrying jobs that are due become ready.
local due = redis.call('ZRANGEBYSCORE', K_DELAYED, '-inf', num(now), 'LIMIT', 0, limit)
for _, id in ipairs(due) do
  redis.call('ZREM', K_DELAYED, id)
  local key = JOB_PREFIX .. id
  if redis.call('EXISTS', key) == 1 then
    make_ready(id, key)
    promoted = promoted + 1
  end
end

-- Expired leases mean the worker died or stalled. The job goes back to ready, or
-- to the dead letter queue when that delivery was its last attempt.
local expired = redis.call('ZRANGEBYSCORE', K_ACTIVE, '-inf', num(now), 'LIMIT', 0, limit)
for _, id in ipairs(expired) do
  redis.call('ZREM', K_ACTIVE, id)
  local key = JOB_PREFIX .. id
  if redis.call('EXISTS', key) == 1 then
    local values = redis.call('HMGET', key, 'attempts', 'max_attempts', 'lease_owner')
    local owner = values[3] or 'unknown'
    redis.call('HSET', key, 'last_error', 'lease expired, last owner ' .. owner)
    redis.call('HDEL', key, 'lease_owner', 'lease_until')
    if tonumber(values[1]) >= tonumber(values[2]) then
      make_dead(id, key, now)
      buried = buried + 1
    else
      make_ready(id, key)
      bump('redelivered')
      reclaimed = reclaimed + 1
    end
  end
end

-- Completed job hashes expire on their own. This drops their index entries.
local trimmed = 0
if retention > 0 then
  trimmed = redis.call('ZREMRANGEBYSCORE', K_COMPLETED, '-inf', num(now - retention))
end

if promoted + reclaimed > 0 then
  wake()
end
return { promoted, reclaimed, buried, trimmed }
