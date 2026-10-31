"""Binance USD-M Futures REST client (public market data).

Rate limiting: tracks `X-MBX-USED-WEIGHT-1M` from every response and keeps usage under a
configurable fraction of the exchange's REQUEST_WEIGHT/minute limit, pausing until the next
minute window when the budget is reached. 429 -> honours Retry-After and backs off; 418 (IP ban)
-> waits Retry-After; 451 -> raises GeoBlocked (non-retryable: the host's jurisdiction is refused).
"""
from __future__ import annotations
import logging, os, threading, time
from typing import Any, Iterator
import requests
from .retry import NonRetryable, backoff_sleep

log = logging.getLogger("goldflow.rest")

class GeoBlocked(NonRetryable):
    pass

class BinanceFuturesREST:
    WEIGHTS = {"/fapi/v1/ping": 1, "/fapi/v1/time": 1, "/fapi/v1/exchangeInfo": 1, "/fapi/v1/aggTrades": 20,
               "/fapi/v1/trades": 5, "/fapi/v1/historicalTrades": 20, "/fapi/v1/klines": 5}

    def __init__(self, base_url: str = "https://fapi.binance.com", weight_limit_per_min: int = 2400,
                 weight_budget_fraction: float = 0.6, max_attempts: int = 8, backoff_base_s: float = 1.0,
                 backoff_cap_s: float = 60.0, timeout_s: int = 30, api_key_env: str = "BINANCE_API_KEY"):
        self.base = base_url.rstrip("/")
        self.budget = int(weight_limit_per_min * weight_budget_fraction)
        self.max_attempts, self.bb, self.bc, self.timeout = max_attempts, backoff_base_s, backoff_cap_s, timeout_s
        self.s = requests.Session()
        self.s.headers["User-Agent"] = "goldflow-acquisition/0.2"
        key = os.environ.get(api_key_env)
        if key: self.s.headers["X-MBX-APIKEY"] = key   # only needed for historicalTrades
        self.has_key = bool(key)
        self._lock = threading.Lock()
        self.used_weight = 0; self._window = int(time.time() // 60)
        self.requests_made = 0

    # ---------------------------------------------------------------- core
    def _throttle(self, w: int) -> None:
        with self._lock:
            win = int(time.time() // 60)
            if win != self._window: self._window, self.used_weight = win, 0
            if self.used_weight + w > self.budget:
                wait = 60 - (time.time() % 60) + 0.5
                log.info("rate budget %d/%d reached; sleeping %.1fs", self.used_weight, self.budget, wait)
                time.sleep(wait); self._window, self.used_weight = int(time.time() // 60), 0
            self.used_weight += w

    def get(self, path: str, params: dict | None = None) -> Any:
        w = self.WEIGHTS.get(path, 10)
        last = None
        for attempt in range(self.max_attempts):
            self._throttle(w)
            try:
                r = self.s.get(self.base + path, params=params, timeout=self.timeout)
                self.requests_made += 1
                uw = r.headers.get("X-MBX-USED-WEIGHT-1M") or r.headers.get("x-mbx-used-weight-1m")
                if uw:
                    with self._lock: self.used_weight = max(self.used_weight, int(uw))
                if r.status_code == 451:
                    raise GeoBlocked(f"HTTP 451 from {self.base}: service unavailable from this location", 451)
                if r.status_code in (429, 418):
                    ra = float(r.headers.get("Retry-After", 60) or 60)
                    log.warning("HTTP %s rate limited; Retry-After %.0fs", r.status_code, ra)
                    time.sleep(ra); last = f"HTTP {r.status_code}"; continue
                if r.status_code >= 500:
                    last = f"HTTP {r.status_code}"; backoff_sleep(attempt, self.bb, self.bc); continue
                if r.status_code >= 400:
                    raise NonRetryable(f"HTTP {r.status_code} {path}: {r.text[:200]}", r.status_code)
                return r.json()
            except NonRetryable:
                raise
            except (requests.ConnectionError, requests.Timeout, ValueError) as e:
                last = repr(e); backoff_sleep(attempt, self.bb, self.bc)
        raise RuntimeError(f"GET {path} failed after {self.max_attempts} attempts ({last})")

    # ---------------------------------------------------------------- endpoints
    def ping(self) -> dict: return self.get("/fapi/v1/ping")
    def server_time(self) -> int: return int(self.get("/fapi/v1/time")["serverTime"])
    def exchange_info(self) -> dict: return self.get("/fapi/v1/exchangeInfo")

    def symbol_spec(self, symbol: str) -> dict:
        info = self.exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                f = {x["filterType"]: x for x in s["filters"]}
                return {"symbol": symbol, "status": s["status"], "contract_type": s["contractType"],
                        "underlying_type": s.get("underlyingType"), "underlying_sub_type": s.get("underlyingSubType"),
                        "base_asset": s["baseAsset"], "quote_asset": s["quoteAsset"], "margin_asset": s["marginAsset"],
                        "onboard_date_ms": s.get("onboardDate"), "price_precision": s["pricePrecision"],
                        "quantity_precision": s["quantityPrecision"], "tick_size": f["PRICE_FILTER"]["tickSize"],
                        "step_size": f["LOT_SIZE"]["stepSize"], "min_qty": f["LOT_SIZE"]["minQty"],
                        "max_qty": f["LOT_SIZE"]["maxQty"], "min_notional": f.get("MIN_NOTIONAL", {}).get("notional"),
                        "server_time_ms": info.get("serverTime"), "rate_limits": info.get("rateLimits"), "raw": s}
        raise LookupError(f"{symbol} not listed on {self.base}")

    def agg_trades(self, symbol: str, from_id: int | None = None, start_ms: int | None = None,
                   end_ms: int | None = None, limit: int = 1000) -> list[dict]:
        p: dict[str, Any] = {"symbol": symbol, "limit": limit}
        if from_id is not None: p["fromId"] = from_id
        if start_ms is not None: p["startTime"] = start_ms
        if end_ms is not None: p["endTime"] = end_ms
        return self.get("/fapi/v1/aggTrades", p)

    def historical_trades(self, symbol: str, from_id: int, limit: int = 500) -> list[dict]:
        if not self.has_key:
            raise NonRetryable("historicalTrades requires an API key (MARKET_DATA); set BINANCE_API_KEY")
        return self.get("/fapi/v1/historicalTrades", {"symbol": symbol, "fromId": from_id, "limit": limit})

    def iter_agg_trades(self, symbol: str, start_ms: int, end_ms: int, from_id: int | None = None) -> Iterator[list[dict]]:
        """Pages of aggTrades covering [start_ms, end_ms), paginated by id (no time-window limits)."""
        if from_id is None:
            first = self.agg_trades(symbol, start_ms=start_ms, end_ms=min(end_ms, start_ms + 3_600_000) - 1)
            probe = start_ms
            while not first and probe < end_ms:   # empty hour: step forward an hour at a time
                probe += 3_600_000
                first = self.agg_trades(symbol, start_ms=probe, end_ms=min(end_ms, probe + 3_600_000) - 1)
            if not first: return
            from_id = first[0]["a"]
        while True:
            page = self.agg_trades(symbol, from_id=from_id)
            if not page: return
            keep = [x for x in page if x["T"] < end_ms]
            if keep: yield keep
            if len(keep) < len(page) or len(page) < 1000:  # reached end of interval or live edge
                return
            from_id = page[-1]["a"] + 1
