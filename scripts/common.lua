-- Prepended to every script when it is loaded, so each script stays one atomic
-- unit on the server. Every script receives the same KEYS and the same first
-- two ARGV entries. Script specific arguments start at ARGV[3].
local K_READY, K_DELAYED, K_ACTIVE = KEYS[1], KEYS[2], KEYS[3]
local K_DEAD, K_COMPLETED = KEYS[4], KEYS[5]
local K_SEQ, K_STATS, K_QUEUES = KEYS[6], KEYS[7], KEYS[8]
local JOB_PREFIX, WAKE_CHANNEL = ARGV[1], ARGV[2]

-- Ready score is (9 - priority) * 2^48 + sequence. The largest value is below
-- 2^53, so a double holds it exactly.
local PRIORITY_BAND = 281474976710656

-- Redis TIME is the only clock used for leases and schedules. Worker clocks can
-- drift apart without changing when a lease expires.
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- tostring() would turn these values into scientific notation and drop digits.
local function num(n)
  return string.format('%.0f', n)
end

local function bump(field)
  redis.call('HINCRBY', K_STATS, field, 1)
end

local function wake()
  redis.call('PUBLISH', WAKE_CHANNEL, '1')
end

local function make_ready(id, key)
  local priority = tonumber(redis.call('HGET', key, 'priority'))
  local seq = redis.call('INCR', K_SEQ)
  redis.call('ZADD', K_READY, num((9 - priority) * PRIORITY_BAND + seq), id)
  redis.call('HSET', key, 'state', 'ready')
end

local function make_dead(id, key, now)
  redis.call('ZADD', K_DEAD, num(now), id)
  redis.call('HSET', key, 'state', 'dead', 'failed_at', num(now))
  bump('dead')
end

-- Reports the job state only when token is the delivery that currently holds
-- the job. A stale worker gets nil and changes nothing.
local function held_state(key, token)
  local values = redis.call('HMGET', key, 'state', 'delivery')
  if not values[1] or values[2] ~= token then
    return nil
  end
  return values[1]
end
