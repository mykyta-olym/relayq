-- ARGV[3] lease_ms, then id and delivery token pairs.
-- Renews leases for every job a worker holds in one round trip. A lease that has
-- passed its deadline but has not been reclaimed is still held exclusively, so
-- renewing it is safe. Returns 1 for a renewed job and 0 for a lost one.
local lease_ms = tonumber(ARGV[3])
local deadline = now_ms() + lease_ms
local out = {}

for i = 4, #ARGV, 2 do
  local id, token = ARGV[i], ARGV[i + 1]
  local key = JOB_PREFIX .. id
  if held_state(key, token) == 'active' then
    redis.call('ZADD', K_ACTIVE, 'XX', num(deadline), id)
    redis.call('HSET', key, 'lease_until', num(deadline))
    out[#out + 1] = 1
  else
    out[#out + 1] = 0
  end
end
return out
