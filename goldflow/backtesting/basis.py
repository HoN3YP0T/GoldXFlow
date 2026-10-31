"""Phase 12: basis between the signal venue (Binance XAUUSDT perpetual) and the execution venue (broker XAUUSD).

* **Sample**, once per Binance base bar (1 s): ``basis = Binance close - broker mid``, where the broker mid is the last
  broker quote strictly before the bar's end. Binance publishes no historical bid/ask, so its last trade price stands
  for its price; this is stated in every report.
* **Estimate at time T**: the median (lower median for an even count) of the samples whose second ended in
  ``(T - window, T]``. It needs ``min_samples`` samples, else the basis is unknown and no order is placed. Only samples
  that ended by T are used: the model asserts it is never queried for a time before its latest sample would allow.
* **Translation** of a signal level: ``broker price = Binance price - basis(T_signal)``, rounded to the nearest broker
  point (half up). The basis is frozen at the signal time for that order's entry reference, stop and target.

All arithmetic is in integer *half-units* of a common unit ``10^-max(2, digits)`` USD (half-units because a mid is the
average of two quotes), so results are exact and identical on every machine.
"""

from __future__ import annotations

from collections import deque
from typing import Any

import numpy as np
from numpy.typing import NDArray


class BasisModel:
    def __init__(self, broker_digits: int, window_ms: int, min_samples: int, binance_decimals: int = 2) -> None:
        self.U = max(binance_decimals, broker_digits)
        self.kb = 10 ** (self.U - binance_decimals)  # Binance tick -> unit
        self.kr = 10 ** (self.U - broker_digits)  # broker point -> unit
        self.window = window_ms
        self.min_samples = min_samples
        self.samples: deque[tuple[int, int]] = deque()  # (second end ms, basis in half-units)
        self.last_query = -(2**62)

    def add(self, sec_end: NDArray[Any], binance_close: NDArray[Any], bid: NDArray[Any], ask: NDArray[Any]) -> None:
        """Samples for seconds ending at ``sec_end`` (ascending) with the broker quote in force just before each."""
        h = 2 * binance_close.astype(np.int64) * self.kb - (bid.astype(np.int64) + ask.astype(np.int64)) * self.kr
        for t, v in zip(sec_end.tolist(), h.tolist()):
            if self.samples and t <= self.samples[-1][0]:
                raise AssertionError("basis samples must arrive in time order")
            if t <= self.last_query:
                raise AssertionError("a basis sample arrived for a time already queried (look-ahead)")
            self.samples.append((t, v))

    def at(self, T: int) -> tuple[int | None, int]:
        """(basis in half-units, number of samples) using only samples that ended in (T - window, T]."""
        self.last_query = max(self.last_query, T)
        while self.samples and self.samples[0][0] <= T - self.window - 3_600_000:
            self.samples.popleft()  # keep an hour of slack for out-of-order queries within an advance
        vals = sorted(v for t, v in self.samples if T - self.window < t <= T)
        if len(vals) < self.min_samples:
            return None, len(vals)
        return vals[(len(vals) - 1) // 2], len(vals)

    def to_broker(self, binance_ticks: int, basis_half: int) -> int:
        """Broker points for a Binance price, rounded half up."""
        num = 2 * binance_ticks * self.kb - basis_half  # half-units of the broker price
        den = 2 * self.kr
        return int((num + den // 2) // den)

    def state(self) -> dict[str, Any]:
        return {"samples": [list(x) for x in self.samples], "last_query": self.last_query}

    def load(self, s: dict[str, Any]) -> None:
        self.samples = deque((int(a), int(b)) for a, b in s["samples"])
        self.last_query = int(s["last_query"])
