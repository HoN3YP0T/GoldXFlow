"""Has the history been downloaded completely? An offline check that makes **no network call**.

``acquire`` writes ``data/state/download_complete.json`` **only** when a run finishes with no failed unit. The record
holds the frozen range, the datasets, and the counts of verified and permanently-missing files.

``data-status`` reports ``complete`` when:
1. the frozen range in ``data/state/range.json`` exists;
2. the completion record covers exactly that range, and every configured dataset;
3. every verified archive file is still on disk with the size recorded when it was verified. If the project folder was
   moved, the file is looked up under the new data root.

Exit codes: 0 = complete (nothing to download); 1 = not started or incomplete (``download_data.bat`` downloads or
resumes, skipping every file that is already verified).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MARKER = "download_complete.json"


def _range(cfg: Any) -> dict[str, Any] | None:
    p = Path(cfg.data_root) / "state" / "range.json"
    return json.loads(p.read_text()) if p.exists() else None


def _datasets(cfg: Any) -> list[str]:
    return sorted(
        k for k, v in (cfg.raw.get("datasets") or {}).items() if not k.startswith("_") and (v or {}).get("enabled", True)
    )


def record_complete(cfg: Any, result: dict[str, Any], datasets: list[str] | None, with_rest: bool) -> Path | None:
    """Called by ``acquire`` after a run. Writes the record only when nothing failed and every dataset was in scope."""
    rng = _range(cfg)
    if rng is None or result.get("failed") or datasets:  # a partial (--datasets) run never certifies the whole download
        return None
    counts: dict[str, int] = {}
    db = Path(cfg.state_db)
    if db.exists():
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        try:
            counts = {str(s): int(n) for s, n in con.execute("SELECT status, COUNT(*) FROM units GROUP BY status")}
        finally:
            con.close()
    rec = {"start_ms": rng["start_ms"], "end_ms": rng["end_ms"], "start": rng["start"], "end": rng["end"], "datasets": _datasets(cfg),
           "units_by_status": counts, "with_rest": with_rest, "run_id": result.get("run_id"),
           "finished_utc": datetime.now(timezone.utc).isoformat()}  # fmt: skip
    p = Path(cfg.data_root) / "state" / MARKER
    p.write_text(json.dumps(rec, indent=1))
    return p


def _local(cfg: Any, local_path: str) -> Path:
    p = Path(local_path)
    if p.exists():
        return p
    parts = p.parts
    if "raw" in parts:  # project folder moved: same file under the current data root
        return Path(cfg.data_root).joinpath(*parts[parts.index("raw") :])
    return p


def data_status(cfg: Any) -> dict[str, Any]:
    rng = _range(cfg)
    out: dict[str, Any] = {"data_root": str(cfg.data_root), "range": None if rng is None else [rng["start"], rng["end"]]}
    if rng is None:
        return {**out, "complete": False, "state": "NOT_STARTED", "reason": "no frozen range yet: nothing has been downloaded"}
    mp = Path(cfg.data_root) / "state" / MARKER
    db = Path(cfg.state_db)
    if not db.exists():
        return {**out, "complete": False, "state": "NOT_STARTED", "reason": "no acquisition catalog yet"}
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        by = {str(s): int(n) for s, n in con.execute("SELECT status, COUNT(*) FROM units GROUP BY status")}
        files = con.execute(
            "SELECT local_path, bytes FROM units WHERE method='archive' AND status='verified' AND local_path IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return {**out, "complete": False, "state": "NOT_STARTED", "reason": "acquisition catalog has no units yet"}
    finally:
        con.close()
    out["units_by_status"] = by
    missing_files = [
        lp for lp, nb in files if not _local(cfg, lp).exists() or (nb is not None and _local(cfg, lp).stat().st_size != int(nb))
    ]
    out["verified_files"] = len(files)
    if missing_files:
        return {**out, "complete": False, "state": "FILES_MISSING", "reason": f"{len(missing_files)} verified files are gone or changed on disk",
                "examples": missing_files[:3]}  # fmt: skip
    if not mp.exists():
        return {
            **out,
            "complete": False,
            "state": "INCOMPLETE",
            "reason": "no completed download recorded for this range (interrupted, or failures left)",
        }
    m = json.loads(mp.read_text())
    if (m["start_ms"], m["end_ms"]) != (rng["start_ms"], rng["end_ms"]):
        return {
            **out,
            "complete": False,
            "state": "INCOMPLETE",
            "reason": f"the recorded download covers {m['start']} -> {m['end']}, not the frozen range",
        }
    if sorted(m.get("datasets", [])) != _datasets(cfg):
        return {
            **out,
            "complete": False,
            "state": "INCOMPLETE",
            "reason": f"datasets changed since the download: {m.get('datasets')} -> {_datasets(cfg)}",
        }
    if any(by.get(s) for s in ("downloading", "downloaded", "failed")):
        return {**out, "complete": False, "state": "INCOMPLETE", "reason": f"units still open: {by}"}
    return {**out, "complete": True, "state": "COMPLETE", "finished_utc": m["finished_utc"], "with_rest": m.get("with_rest"),
            "reason": "every file for the frozen range is downloaded and verified; nothing to download"}  # fmt: skip
