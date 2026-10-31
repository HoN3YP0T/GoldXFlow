"""Versioned database migrations and schema-version policy (Phase 1).

Policy
------
* SQLite (catalog/checkpoints/run records): every structural change after the baseline is a numbered migration
  appended to MIGRATIONS. Migrations are applied in order, once, inside a transaction, and recorded in
  `schema_migrations` with a checksum of their SQL. Editing an applied migration is refused (checksum mismatch).
* Parquet event schemas (goldflow/schemas.py): SCHEMA_VERSION is bumped for any change to columns, types or
  meaning. A bump requires (1) a note in docs/SCHEMAS_V<n>.md describing the change, (2) a migration entry here
  of kind 'parquet' recording it, and (3) regenerating partitions with `python -m goldflow.cli normalize --reset`.
  Readers check the file's `goldflow.schema_version` metadata and refuse unknown versions.

Version 1 is the baseline: the tables created idempotently by checkpoint.py, catalog.py and replay.py as of
Phases 2-4. It is recorded, not executed.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Migration:
    version: int
    kind: str  # 'sqlite' | 'parquet' | 'baseline'
    description: str
    sql: str = ""

    @property
    def checksum(self) -> str:
        return hashlib.sha256(f"{self.version}|{self.kind}|{self.sql}".encode()).hexdigest()[:16]


MIGRATIONS: list[Migration] = [
    Migration(
        1,
        "baseline",
        "Phases 2-4 baseline: units/gaps/rest_cursor/events/runs (checkpoint.py), raw_files/"
        "partitions/day_status/hour_status/quarantine_counts/raw_day_stats/crosscheck/validation_runs/"
        "day_status_pre/hour_status_pre/day_boundary/day_runs (catalog.py), replay_checkpoints (replay.py); "
        "Parquet schemas v1 (TradeEvent, AggTradeEvent, DepthBandSnapshot, MarketMetrics, Kline1m, DepthEvent)",
    ),
]

_TABLE = """CREATE TABLE IF NOT EXISTS schema_migrations(
  version INTEGER PRIMARY KEY, kind TEXT, description TEXT, checksum TEXT, applied_utc TEXT)"""


class MigrationError(RuntimeError):
    pass


def current_version(db: sqlite3.Connection) -> int:
    db.execute(_TABLE)
    r = db.execute("SELECT MAX(version) FROM schema_migrations").fetchone()
    return int(r[0]) if r and r[0] is not None else 0


def apply_migrations(db: sqlite3.Connection, migrations: list[Migration] | None = None) -> list[int]:
    """Apply pending migrations; verify applied ones were not edited. Returns the versions applied now."""
    migs = sorted(migrations or MIGRATIONS, key=lambda m: m.version)
    if [m.version for m in migs] != list(range(1, len(migs) + 1)):
        raise MigrationError("migration versions must be 1..N without gaps")
    db.execute(_TABLE)
    applied = {int(v): c for v, c in db.execute("SELECT version, checksum FROM schema_migrations")}
    for v, c in applied.items():
        m = next((x for x in migs if x.version == v), None)
        if m is None:
            raise MigrationError(f"database has migration {v} unknown to this code (newer code was used?)")
        if m.checksum != c:
            raise MigrationError(f"migration {v} was modified after being applied (checksum {c} != {m.checksum})")
    done = []
    for m in migs:
        if m.version in applied:
            continue
        in_tx = db.in_transaction
        if not in_tx:
            db.execute("BEGIN IMMEDIATE")
        try:
            if m.kind == "sqlite" and m.sql:
                for stmt in [s for s in m.sql.split(";") if s.strip()]:
                    db.execute(stmt)
            db.execute(
                "INSERT INTO schema_migrations VALUES(?,?,?,?,?)",
                (m.version, m.kind, m.description, m.checksum, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            )
            if not in_tx:
                db.execute("COMMIT")
        except Exception:
            if not in_tx:
                db.execute("ROLLBACK")
            raise
        done.append(m.version)
    return done
