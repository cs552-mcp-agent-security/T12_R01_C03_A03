"""Token-bucket rate limiter backed by Redis (pipelined GET/SET form)."""
from __future__ import annotations

import time
from typing import Tuple

import redis


class Limiter:
    def __init__(self, redis_url: str, capacity: int, refill_per_sec: float) -> None:
        self._r = redis.Redis.from_url(redis_url, decode_responses=False)
        self._capacity = capacity
        self._refill = refill_per_sec

    def check(self, key: str, cost: int = 1) -> Tuple[bool, int]:
        bucket_key = f"rl:{{{key}}}"
        pipe = self._r.pipeline()
        pipe.hmget(bucket_key, "tokens", "ts")
        pipe.time()
        state, now = pipe.execute()
        now_ms = int(now[0]) * 1000 + int(now[1]) // 1000
        if state[0] is None:
            tokens = float(self._capacity)
            last_ts = now_ms
        else:
            tokens = float(state[0]); last_ts = int(state[1])
        elapsed = max(0.0, (now_ms - last_ts) / 1000.0)
        tokens = min(self._capacity, tokens + elapsed * self._refill)
        allowed = False
        if tokens >= cost:
            tokens -= cost
            allowed = True
        ttl = max(60, int(self._capacity / self._refill * 10))
        pipe = self._r.pipeline()
        pipe.hmset(bucket_key, {"tokens": str(tokens), "ts": str(now_ms)})
        pipe.expire(bucket_key, ttl)
        pipe.execute()
        return allowed, int(tokens)
