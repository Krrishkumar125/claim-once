// Every state change is one Lua script on one key, so Redis runs it atomically.
// Record fields: s state, t fencing token, o owner, l lease expiry (ms, Redis clock),
// b effect began (0/1), a attempts, r result, e error, c created, u updated.

const NOW = `
local k = KEYS[1]
local tm = redis.call('TIME')
local now = tonumber(tm[1]) * 1000 + math.floor(tonumber(tm[2]) / 1000)
`;

export const RESERVE = `${NOW}
local owner = ARGV[1]
local lease = tonumber(ARGV[2])
local keep = tonumber(ARGV[3])
local s = redis.call('HGET', k, 's')
if s == 'done' then return {'done', redis.call('HGET', k, 'r') or '', redis.call('HGET', k, 't') or '0'} end
if s == 'failed' then return {'failed', redis.call('HGET', k, 'e') or '', redis.call('HGET', k, 't') or '0'} end
if s == 'in_doubt' then return {'in_doubt', redis.call('HGET', k, 't') or '0', redis.call('HGET', k, 'o') or ''} end
local takeover = '0'
if s == 'pending' then
  local l = tonumber(redis.call('HGET', k, 'l') or '0')
  if l > now then
    return {'busy', redis.call('HGET', k, 't') or '0', redis.call('HGET', k, 'o') or '', tostring(l - now)}
  end
  if redis.call('HGET', k, 'b') == '1' then
    redis.call('HSET', k, 's', 'in_doubt', 'u', now)
    return {'in_doubt', redis.call('HGET', k, 't') or '0', redis.call('HGET', k, 'o') or ''}
  end
  takeover = '1'
end
local t = redis.call('HINCRBY', k, 't', 1)
local a = redis.call('HINCRBY', k, 'a', 1)
redis.call('HSET', k, 's', 'pending', 'o', owner, 'l', now + lease, 'b', '0', 'u', now)
redis.call('HSETNX', k, 'c', now)
redis.call('HDEL', k, 'r', 'e')
redis.call('PEXPIRE', k, keep)
return {'reserved', tostring(t), takeover, tostring(a)}
`;

const OWNED = `
if redis.call('HGET', k, 't') ~= ARGV[1] then return 'stale' end
local s = redis.call('HGET', k, 's')
`;

export const BEGIN = `${NOW}${OWNED}
if s ~= 'pending' then return s end
redis.call('HSET', k, 'b', '1', 'l', now + tonumber(ARGV[2]), 'u', now)
return 'ok'
`;

export const HEARTBEAT = `${NOW}${OWNED}
if s ~= 'pending' then return s end
redis.call('HSET', k, 'l', now + tonumber(ARGV[2]), 'u', now)
return 'ok'
`;

export const COMPLETE = `${NOW}${OWNED}
if s ~= 'pending' and s ~= 'in_doubt' then return s end
redis.call('HSET', k, 's', 'done', 'r', ARGV[2], 'u', now)
redis.call('HDEL', k, 'l', 'e')
redis.call('PEXPIRE', k, tonumber(ARGV[3]))
return 'ok'
`;

// ARGV: token, error, maxAttempts (0 = no limit)
export const RELEASE = `${NOW}${OWNED}
if s ~= 'pending' and s ~= 'in_doubt' then return s end
local a = tonumber(redis.call('HGET', k, 'a') or '0')
local max = tonumber(ARGV[3])
if max > 0 and a >= max then
  redis.call('HSET', k, 's', 'failed', 'e', ARGV[2], 'u', now)
  redis.call('HDEL', k, 'l')
  return 'failed'
end
redis.call('HSET', k, 's', 'free', 'e', ARGV[2], 'b', '0', 'u', now)
redis.call('HDEL', k, 'l')
return 'released'
`;

// ARGV: token, error
export const FAIL = `${NOW}${OWNED}
if s ~= 'pending' and s ~= 'in_doubt' then return s end
redis.call('HSET', k, 's', 'failed', 'e', ARGV[2], 'u', now)
redis.call('HDEL', k, 'l')
return 'ok'
`;

// ARGV: token, error
export const MARK_IN_DOUBT = `${NOW}${OWNED}
if s ~= 'pending' then return s end
redis.call('HSET', k, 's', 'in_doubt', 'e', ARGV[2], 'u', now)
return 'ok'
`;

// ARGV: token, outcome (done | not_happened | failed), payload, retentionMs
export const RESOLVE = `${NOW}${OWNED}
if s ~= 'in_doubt' then return s end
local outcome = ARGV[2]
if outcome == 'done' then
  redis.call('HSET', k, 's', 'done', 'r', ARGV[3], 'u', now)
  redis.call('HDEL', k, 'l', 'e')
  redis.call('PEXPIRE', k, tonumber(ARGV[4]))
elseif outcome == 'not_happened' then
  redis.call('HSET', k, 's', 'free', 'b', '0', 'u', now)
  redis.call('HDEL', k, 'l')
else
  redis.call('HSET', k, 's', 'failed', 'e', ARGV[3], 'u', now)
  redis.call('HDEL', k, 'l')
end
return 'ok'
`;
