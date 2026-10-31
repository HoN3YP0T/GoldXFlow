"""Crash-safe SQLite checkpoint store.

Every planned unit of work (an archive file or a REST segment) is a row in `units`.
Status lifecycle:  planned -> downloading -> downloaded -> verified
                                         \-> failed (retryable on next run)
                                         \-> missing (source has no data; reason recorded)
Gaps (missing intervals / ids) are recorded in `gaps` with an explicit reason.
Nothing is ever deleted silently; superseded file versions are logged in `events`.
"""
from __future__ import annotations
import json, sqlite3, threading, time
from pathlib import Path
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs(
  run_id TEXT PRIMARY KEY, started_utc TEXT, finished_utc TEXT,
  range_start_ms INTEGER, range_end_ms INTEGER, config_json TEXT, status TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS units(
  unit_id TEXT PRIMARY KEY,           -- e.g. archive:trades:monthly:2026-05
  source TEXT, method TEXT,           -- method: archive | rest
  symbol TEXT, market TEXT, dataset TEXT, granularity TEXT, period TEXT,
  url TEXT, local_path TEXT,
  interval_start_ms INTEGER, interval_end_ms INTEGER,   -- requested coverage [start,end)
  status TEXT, http_status INTEGER, attempts INTEGER DEFAULT 0,
  bytes INTEGER, sha256 TEXT, expected_sha256 TEXT, remote_etag TEXT, remote_last_modified TEXT, remote_size INTEGER,
  records INTEGER, first_id INTEGER, last_id INTEGER, first_ts INTEGER, last_ts INTEGER,
  id_gap_count INTEGER, id_gap_ids INTEGER, dup_ids INTEGER, ts_backwards INTEGER,
  verify_json TEXT, error TEXT, run_id TEXT, updated_utc TEXT, completed_utc TEXT);
CREATE TABLE IF NOT EXISTS gaps(
  gap_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, dataset TEXT, kind TEXT,
  start_ms INTEGER, end_ms INTEGER, start_id INTEGER, end_id INTEGER, size INTEGER,
  unit_id TEXT, reason TEXT, recoverable TEXT, detail_json TEXT);
CREATE TABLE IF NOT EXISTS rest_cursor(
  cursor_id TEXT PRIMARY KEY, last_id INTEGER, last_ts INTEGER, pages INTEGER, records INTEGER, updated_utc TEXT);
CREATE TABLE IF NOT EXISTS events(
  ts_utc TEXT, run_id TEXT, level TEXT, unit_id TEXT, message TEXT, detail_json TEXT);
"""

def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

class Checkpoint:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        from .fsutil import connect
        self.db = connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)

    # -- generic helpers -------------------------------------------------
    def _exec(self, sql: str, args: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                cur = self.db.execute(sql, tuple(args)); self.db.execute("COMMIT"); return cur
            except Exception:
                self.db.execute("ROLLBACK"); raise

    def query(self, sql: str, args: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self.db.execute(sql, tuple(args)).fetchall()

    # -- runs -------------------------------------------------------------
    def start_run(self, run_id: str, start_ms: int, end_ms: int, cfg: dict) -> None:
        self._exec("INSERT INTO runs VALUES(?,?,?,?,?,?,?,?)",
                   (run_id, _now(), None, start_ms, end_ms, json.dumps(cfg), "running", None))

    def finish_run(self, run_id: str, status: str, notes: str = "") -> None:
        self._exec("UPDATE runs SET finished_utc=?, status=?, notes=? WHERE run_id=?", (_now(), status, notes, run_id))

    # -- units ------------------------------------------------------------
    def get_unit(self, unit_id: str) -> sqlite3.Row | None:
        r = self.query("SELECT * FROM units WHERE unit_id=?", (unit_id,))
        return r[0] if r else None

    def upsert_unit(self, unit_id: str, **fields: Any) -> None:
        fields["updated_utc"] = _now()
        existing = self.get_unit(unit_id)
        if existing is None:
            fields["unit_id"] = unit_id
            cols = ",".join(fields); qs = ",".join("?" * len(fields))
            self._exec(f"INSERT INTO units({cols}) VALUES({qs})", fields.values())
        else:
            sets = ",".join(f"{k}=?" for k in fields)
            self._exec(f"UPDATE units SET {sets} WHERE unit_id=?", [*fields.values(), unit_id])

    def bump_attempts(self, unit_id: str) -> None:
        self._exec("UPDATE units SET attempts=COALESCE(attempts,0)+1 WHERE unit_id=?", (unit_id,))

    # -- gaps / events ----------------------------------------------------
    def clear_gaps(self, unit_id: str) -> None:
        self._exec("DELETE FROM gaps WHERE unit_id=?", (unit_id,))

    def add_gap(self, run_id: str, dataset: str, kind: str, reason: str, recoverable: str,
                unit_id: str | None = None, start_ms: int | None = None, end_ms: int | None = None,
                start_id: int | None = None, end_id: int | None = None, size: int | None = None,
                detail: dict | None = None) -> None:
        self._exec("INSERT INTO gaps(run_id,dataset,kind,start_ms,end_ms,start_id,end_id,size,unit_id,reason,recoverable,detail_json)"
                   " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                   (run_id, dataset, kind, start_ms, end_ms, start_id, end_id, size, unit_id, reason, recoverable,
                    json.dumps(detail) if detail else None))

    def event(self, run_id: str, level: str, message: str, unit_id: str | None = None, detail: dict | None = None) -> None:
        self._exec("INSERT INTO events VALUES(?,?,?,?,?,?)",
                   (_now(), run_id, level, unit_id, message, json.dumps(detail) if detail else None))

    # -- REST cursors -----------------------------------------------------
    def get_cursor(self, cursor_id: str) -> sqlite3.Row | None:
        r = self.query("SELECT * FROM rest_cursor WHERE cursor_id=?", (cursor_id,))
        return r[0] if r else None

    def set_cursor(self, cursor_id: str, last_id: int, last_ts: int, pages: int, records: int) -> None:
        self._exec("INSERT INTO rest_cursor VALUES(?,?,?,?,?,?) ON CONFLICT(cursor_id) DO UPDATE SET "
                   "last_id=excluded.last_id,last_ts=excluded.last_ts,pages=excluded.pages,records=excluded.records,updated_utc=excluded.updated_utc",
                   (cursor_id, last_id, last_ts, pages, records, _now()))
