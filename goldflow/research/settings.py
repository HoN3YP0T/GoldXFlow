"""Phase 13 settings: the ``research`` block (docs/RESEARCH_V1.md). Splits, walk-forward, statistics, approval criteria.

Splits are fixed **before** any testing and stored with every result. The approval thresholds are proposals; the
final numbers are decision D8, and ``max_oos_drawdown`` has no default: until you set it, that criterion reports
``NOT_SET`` and approval cannot pass.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date
from fractions import Fraction
from typing import Any

from ..timeutil import DAY_MS

DEFAULT_RESEARCH: dict[str, Any] = {
    "version": 1,
    "splits": {
        "development": ["2026-04-30", "2026-08-31"],
        "validation": ["2026-09-01", "2026-09-30"],
        "test": ["2026-10-01", "2026-10-29"],
    },
    "walk_forward": {"in_sample_days": 56, "out_of_sample_days": 14, "step_days": 14, "warmup_days": 2},
    "objective": "profit_factor",
    "min_trades_in_sample": 30,
    "min_trades_conclusion": 100,
    "bootstrap": {"samples": 2000, "seed": 7},
    "approval": {
        "oos_profit_factor": "1.2",
        "wf_positive_share": "0.7",
        "max_oos_drawdown": None,
        "param_change": "0.2",
        "cost_multiplier": "1.5",
    },
}
OBJECTIVES = ("profit_factor", "net_cents", "expectancy_r")


def _day(s: str) -> int:
    return (date.fromisoformat(s).toordinal() - date(1970, 1, 1).toordinal()) * DAY_MS


def _frac(v: Any, what: str) -> Fraction:
    if isinstance(v, (bool, float)):
        raise ValueError(f"{what}: give a decimal string")
    return Fraction(str(v))


@dataclass(frozen=True)
class ResearchSettings:
    splits: dict[str, tuple[int, int]]  # name -> [start_ms, end_ms) (end date inclusive in the config)
    is_ms: int
    oos_ms: int
    step_ms: int
    warmup_ms: int
    objective: str
    min_trades_is: int
    min_trades_conclusion: int
    boot_samples: int
    boot_seed: int
    approval: dict[str, Fraction | None]
    raw: dict[str, Any]
    hash: str

    @classmethod
    def from_config(cls, cfg: Any) -> "ResearchSettings":
        user = cfg.raw.get("research") or {}
        raw = {**DEFAULT_RESEARCH, **{k: v for k, v in user.items() if not k.startswith("_")}}
        for blk in ("splits", "walk_forward", "bootstrap", "approval"):
            raw[blk] = {k: v for k, v in {**DEFAULT_RESEARCH[blk], **(user.get(blk) or {})}.items() if not k.startswith("_")}
        bad = sorted(set(raw) - set(DEFAULT_RESEARCH))
        if bad:
            raise ValueError(f"research: unknown keys {bad}")
        sp = {}
        for name in ("development", "validation", "test"):
            a, b = raw["splits"][name]
            sp[name] = (_day(a), _day(b) + DAY_MS)
            if sp[name][1] <= sp[name][0]:
                raise ValueError(f"research.splits.{name}: end before start")
        if not (sp["development"][1] <= sp["validation"][0] and sp["validation"][1] <= sp["test"][0]):
            raise ValueError("research.splits must be chronological and non-overlapping: development < validation < test")
        wf = raw["walk_forward"]
        if raw["objective"] not in OBJECTIVES:
            raise ValueError(f"research.objective must be one of {list(OBJECTIVES)}")
        ap: dict[str, Fraction | None] = {
            k: (None if v is None else _frac(v, f"research.approval.{k}")) for k, v in raw["approval"].items()
        }
        body = json.dumps(raw, sort_keys=True, default=str)
        return cls(sp, int(wf["in_sample_days"]) * DAY_MS, int(wf["out_of_sample_days"]) * DAY_MS, int(wf["step_days"]) * DAY_MS,
                   int(wf["warmup_days"]) * DAY_MS, str(raw["objective"]), int(raw["min_trades_in_sample"]), int(raw["min_trades_conclusion"]),
                   int(raw["bootstrap"]["samples"]), int(raw["bootstrap"]["seed"]), ap, raw, hashlib.sha256(body.encode()).hexdigest())  # fmt: skip
