"""Synthetic Binance-futures-style depth exchange for Phase 8 tests (test-only; never used as market data).

It keeps the *true* book in integer ticks/lots and emits `depthUpdate` events with U/u/pu exactly as Binance documents
them, plus REST-style snapshots. Tests compare the reconstructed local book against this truth.
"""

from __future__ import annotations

import json
import random
import threading

STREAM = "xauusdt@depth@100ms"


def px(t: int) -> str:
    return f"{t // 100}.{t % 100:02d}"


def qx(lots: int) -> str:
    return f"{lots // 1000}.{lots % 1000:03d}"


class SimExchange:
    def __init__(self, seed: int = 7, mid: int = 400_000, levels: int = 60, symbol: str = "XAUUSDT"):
        self.rng = random.Random(seed)
        self.symbol = symbol
        self.mid = mid  # asks > mid, bids < mid (mid itself never quoted)
        self.levels = levels
        self.bids: dict[int, int] = {}
        self.asks: dict[int, int] = {}
        for d in range(1, levels + 1):
            self.bids[mid - d] = self.rng.randint(1, 5000)
            self.asks[mid + d] = self.rng.randint(1, 5000)
        self.u = 10_000
        self.t = 1_788_000_000_000
        self.lock = threading.Lock()

    def step(self) -> dict:
        """Advance the exchange by one diff event (it may contain several internal updates: u - U + 1 >= 1)."""
        with self.lock:
            self.t += self.rng.choice([100, 100, 100, 200, 500])
            changes_b: dict[int, int] = {}
            changes_a: dict[int, int] = {}
            if self.rng.random() < 0.2:  # mid moves; remove levels that would cross
                self.mid += self.rng.choice([-1, 1])
                for p in [p for p in self.bids if p >= self.mid]:
                    del self.bids[p]
                    changes_b[p] = 0
                for p in [p for p in self.asks if p <= self.mid]:
                    del self.asks[p]
                    changes_a[p] = 0
            for _ in range(self.rng.randint(1, 6)):
                d = self.rng.randint(1, self.levels)
                q = 0 if self.rng.random() < 0.25 else self.rng.randint(1, 5000)
                if self.rng.random() < 0.5:
                    p = self.mid - d
                    changes_b[p] = q
                    if q:
                        self.bids[p] = q
                    else:
                        self.bids.pop(p, None)
                else:
                    p = self.mid + d
                    changes_a[p] = q
                    if q:
                        self.asks[p] = q
                    else:
                        self.asks.pop(p, None)
            pu = self.u
            first = self.u + 1
            self.u += self.rng.randint(1, 4)
            return {
                "e": "depthUpdate",
                "E": self.t,
                "T": self.t - 3,
                "s": self.symbol,
                "U": first,
                "u": self.u,
                "pu": pu,
                "b": [[px(p), qx(q)] for p, q in sorted(changes_b.items(), reverse=True)],
                "a": [[px(p), qx(q)] for p, q in sorted(changes_a.items())],
            }

    def snapshot(self, limit: int = 1000) -> dict:
        with self.lock:
            bids = sorted(self.bids.items(), reverse=True)[:limit]
            asks = sorted(self.asks.items())[:limit]
            return {
                "lastUpdateId": self.u,
                "E": self.t + 7,
                "T": self.t + 5,
                "bids": [[px(p), qx(q)] for p, q in bids],
                "asks": [[px(p), qx(q)] for p, q in asks],
            }

    def truth(self) -> tuple[dict[int, int], dict[int, int]]:
        with self.lock:
            return dict(self.bids), dict(self.asks)


def ws(ev: dict, stream: str = STREAM) -> str:
    return json.dumps({"stream": stream, "data": ev}, separators=(",", ":"))
