-- ARGV[3] max jobs, ARGV[4] lease_ms, ARGV[5] worker id.
-- Pop and lease happen in one script. There is no moment where a job is in
-- neither the ready set nor the active set, so a crash cannot drop it.
-- Attempts are counted here and not on failure, so a job that kills its worker
-- still runs out of attempts. Returns one flat HGETALL array per leased job.
local count, lease_ms, worker = tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
local now = now_ms()
local deadline = now + lease_ms
local out = {}

local popped = redis.call('ZPOPMIN', K_READY, count)
for i = 1, #popped, 2 do
  local id = popped[i]
  local key = JOB_PREFIX .. id
  if redis.call('EXISTS', key) == 1 then
    redis.call('HINCRBY', key, 'attempts', 1)
    redis.call('HINCRBY', key, 'delivery', 1)
    redis.call('HSET', key, 'state', 'active', 'lease_owner', worker,
      'lease_until', num(deadline), 'started_at', num(now))
    redis.call('ZADD', K_ACTIVE, num(deadline), id)
    bump('delivered')
    out[#out + 1] = redis.call('HGETALL', key)
  end
end
return out
