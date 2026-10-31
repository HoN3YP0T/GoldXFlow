"""Phase 15: shared base for venues whose history comes from a public daily trade archive (OKX, Bybit) and whose
instrument specification comes from a public REST endpoint. One instance = one venue; nothing here merges venues."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa

from .base import Capability, HealthStatus, InstrumentSpec, Provider, ProviderUnavailable


def get_json(url: str, timeout_s: float = 20.0) -> Any:
    import requests

    from ..venues.archives import UA

    r = requests.get(url, headers=UA, timeout=timeout_s)
    if r.status_code in (403, 451):
        raise ProviderUnavailable(f"HTTP {r.status_code} from {url.split('?')[0]} (geo-restricted from this host)")
    r.raise_for_status()
    return r.json()


class ArchiveVenueProvider(Provider):
    """Subclasses set provider_id, venue_id, DISCOVERY_URL and implement capabilities() and _spec(json)."""

    DISCOVERY_URL = ""
    HEALTH_URL = ""

    def __init__(self, data_root: Path, symbol: str, mapping: Any) -> None:
        self.data_root, self.symbol, self.mapping = Path(data_root), symbol, mapping

    @classmethod
    def from_config(cls, cfg: Any) -> "ArchiveVenueProvider":
        from ..venues.mapping import mappings

        m = mappings(cfg)[cls.provider_id]
        return cls(cfg.data_root, m.symbol, m)

    def _spec(self, j: Any, symbol: str) -> InstrumentSpec:  # pragma: no cover - per venue
        raise NotImplementedError

    def discover_instrument(self, symbol: str) -> InstrumentSpec:
        self.capabilities().require([Capability.INSTRUMENT_DISCOVERY], self.provider_id)
        return self._spec(get_json(self.DISCOVERY_URL.format(symbol=symbol)), symbol)

    def historical_trades(self, symbol: str, start_ms: int, end_ms: int) -> Iterator[pa.RecordBatch]:
        """Raw-schema batches (validator column names) from the venue's daily archive files, kept raw on disk."""
        self.capabilities().require([Capability.HISTORICAL_TRADES], self.provider_id)
        from ..venues.archives import file_interval, get_raw, parse

        off = self.mapping.archive_day_utc_offset_h
        d = (date.fromtimestamp(start_ms / 1000) - timedelta(days=1)).isoformat()
        last = (date.fromtimestamp((end_ms - 1) / 1000) + timedelta(days=1)).isoformat()
        while d <= last:
            a, b = file_interval(self.provider_id, d, off)
            if b > start_ms and a < end_ms:
                p = get_raw(self.data_root, self.provider_id, self.mapping.market_type, symbol, d)
                if p is not None:
                    raw = parse(self.provider_id, p, symbol, d)
                    raw.pop("match_id", None)
                    t = pa.table(raw)
                    ts = t.column("time").to_numpy()
                    keep = (ts >= start_ms) & (ts < end_ms)
                    for batch in t.filter(pa.array(keep)).to_batches(max_chunksize=65536):
                        yield batch
            d = (date.fromisoformat(d) + timedelta(days=1)).isoformat()

    def health(self) -> HealthStatus:
        now = int(time.time() * 1000)
        try:
            get_json(self.HEALTH_URL.format(symbol=self.symbol))
            return HealthStatus("OK", f"{self.HEALTH_URL.split('?')[0]} reachable", now)
        except ProviderUnavailable as e:
            return HealthStatus("DEGRADED", f"REST: {e}; archive may still be reachable", now)
        except Exception as e:  # noqa: BLE001 - health must never raise
            return HealthStatus("DOWN", repr(e), now)
