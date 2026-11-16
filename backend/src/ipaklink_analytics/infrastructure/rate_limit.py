from redis.asyncio import Redis

TOKEN_BUCKET = """
local now = redis.call('TIME')
local ms = now[1] * 1000 + math.floor(now[2] / 1000)
local cap = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local previous = tonumber(state[2]) or ms
local tokens = math.min(cap, (tonumber(state[1]) or cap) + math.max(0, ms - previous) * rate)
local retry = 0
if tokens >= cost then
    tokens = tokens - cost
else
    retry = math.ceil((cost - tokens) / rate / 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'ts', math.max(previous, ms))
redis.call('PEXPIRE', KEYS[1], math.ceil(2 * cap / rate))
return retry
"""


class RateLimiter:
    def __init__(self, redis: Redis, environment: str) -> None:
        self.script = redis.register_script(TOKEN_BUCKET)
        self.prefix = f"analytics:rl:{environment}:"

    async def take(self, key: str, capacity: int, per_minute: int, cost: int = 1) -> int:
        return int(
            await self.script(keys=[self.prefix + key], args=[capacity, per_minute / 60000, cost])
        )
