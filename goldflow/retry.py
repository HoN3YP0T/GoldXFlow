"""Exponential backoff with full jitter, shared by archive and REST clients."""
from __future__ import annotations
import random, time

class NonRetryable(Exception):
    """Error that must not be retried (e.g. 404, 451 legal block, bad request)."""
    def __init__(self, msg: str, status: int | None = None):
        super().__init__(msg); self.status = status

def backoff_sleep(attempt: int, base: float, cap: float, floor: float = 0.0) -> float:
    delay = max(floor, random.uniform(0, min(cap, base * (2 ** attempt))))
    time.sleep(delay); return delay
