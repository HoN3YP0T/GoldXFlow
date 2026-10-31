"""Configuration loading. All timestamps are UTC milliseconds."""
from __future__ import annotations
import json, copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass
class Config:
    raw: dict
    base_dir: Path

    @classmethod
    def load(cls, path: str | Path, validate: bool = True) -> "Config":
        """Load and (by default) validate. Invalid or conflicting settings raise settings.ConfigError."""
        p = Path(path).resolve()
        cfg = cls(json.loads(p.read_text()), p.parent.parent)
        if validate:
            cfg.validate()
        return cfg

    def validate(self, check_discovery: bool = True):
        from .settings import validate_config
        self._settings = validate_config(self.raw, self.base_dir, check_discovery)
        return self._settings

    @property
    def settings(self):
        s = getattr(self, "_settings", None)
        return s if s is not None else self.validate()

    @property
    def config_hash(self) -> str:
        from .settings import config_hash
        return config_hash(self.raw)

    def __getitem__(self, k: str) -> Any:
        return self.raw[k]

    @property
    def symbol(self) -> str: return self.raw["symbol"]
    @property
    def market(self) -> str: return self.raw["market"]
    @property
    def data_root(self) -> Path: return (self.base_dir / self.raw["paths"]["data_root"]).resolve()
    @property
    def state_db(self) -> Path:
        """GOLDFLOW_STATE_DB overrides the location (used on mounts where SQLite cannot run safely)."""
        import os
        env = os.environ.get("GOLDFLOW_STATE_DB")
        return Path(env).resolve() if env else (self.base_dir / self.raw["paths"]["state_db"]).resolve()
    def raw_dir(self, dataset: str, granularity: str) -> Path:
        return self.data_root / "raw" / self.raw["source"] / self.market / self.symbol / dataset / granularity
    def as_dict(self) -> dict: return copy.deepcopy(self.raw)
