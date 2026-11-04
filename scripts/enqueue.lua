-- ARGV[3] idempotency key prefix, ARGV[4] queue name, ARGV[5] job count.
-- Then 12 values per job: id, type, payload, priority, max_attempts, timeout_ms,
-- backoff_base_ms, backoff_max_ms, delay_ms, run_at_ms, idem_key, idem_ttl_ms.
-- Returns one {id, created} pair per job.
local IDEM_PREFIX, QUEUE = ARGV[3], ARGV[4]
local count = tonumber(ARGV[5])
local stride = 12
local now = now_ms()
local out = {}
local any_ready = false

for n = 0, count - 1 do
  local i = 6 + n * stride
  local id, job_type, payload = ARGV[i], ARGV[i + 1], ARGV[i + 2]
  local delay_ms, run_at_ms = tonumber(ARGV[i + 8]), tonumber(ARGV[i + 9])
  local idem, idem_ttl = ARGV[i + 10], ARGV[i + 11]
  local key = JOB_PREFIX .. id

  local existing = false
  if idem ~= '' then
    existing = redis.call('GET', IDEM_PREFIX .. idem)
  end

  if existing then
    bump('deduplicated')
    out[#out + 1] = { existing, 0 }
  elseif redis.call('EXISTS', key) == 1 then
    -- The id is generated before the first send, so this is the Redis client
    -- transparently resending a call whose reply was lost.
    out[#out + 1] = { id, 0 }
  else
    if idem ~= '' then
      redis.call('SET', IDEM_PREFIX .. idem, id, 'PX', idem_ttl)
    end

    local run_at = now
    if delay_ms > 0 then
      run_at = now + delay_ms
    elseif run_at_ms > 0 then
      run_at = run_at_ms
    end

    redis.call('HSET', key,
      'id', id, 'queue', QUEUE, 'type', job_type, 'payload', payload,
      'priority', ARGV[i + 3], 'max_attempts', ARGV[i + 4],
      'timeout_ms', ARGV[i + 5], 'backoff_base_ms', ARGV[i + 6],
      'backoff_max_ms', ARGV[i + 7], 'idempotency_key', idem,
      'attempts', 0, 'delivery', 0,
      'created_at', num(now), 'run_at', num(run_at))

    if run_at > now then
      redis.call('ZADD', K_DELAYED, num(run_at), id)
      redis.call('HSET', key, 'state', 'delayed')
    else
      make_ready(id, key)
      any_ready = true
    end
    bump('enqueued')
    out[#out + 1] = { id, 1 }
  end
end

redis.call('SADD', K_QUEUES, QUEUE)
if any_ready then
  wake()
end
return out
