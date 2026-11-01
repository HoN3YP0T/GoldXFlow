"""Phase 17: backups with verified restore.

``backup(cfg, dest)`` writes a snapshot directory ``<dest>/<UTC timestamp>/`` containing:
- **every file under the data root**: raw archives and captures, normalised partitions, derived datasets, reports,
  research ledger and test lock, execution audit logs and state, live checkpoints, ops alerts. Temporary files
  (``*.tmp``, ``*.part``, ``_staging``) and ``cache/`` are left out;
- **every SQLite database** (Phase 3 catalog, replay checkpoints, venue catalogs). These are copied with the SQLite
  online backup API, so the copy is consistent even while the engine writes, and each copy is checked with
  ``PRAGMA integrity_check``;
- the **configuration file**;
- ``manifest.json``: every file with its size and SHA-256, the database checks, and the manifest's own SHA-256.

**Incremental.** A file whose size, modification time and SHA-256 match the previous snapshot is **hard-linked**
from it rather than copied. Snapshots are independent directories, but unchanged raw data costs no extra space.

``verify_backup(snapshot)`` re-hashes every file against the manifest, re-checks every database, and verifies the
execution audit chains and the research ledger inside the snapshot.

``restore(snapshot, target)`` copies a verified snapshot into an **empty** target data root, which must not be an
existing data root, then verifies the restored files against the manifest again. The tests then rebuild from the
restored root and compare state digests with the original.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SKIP_DIRS = {"cache", "_staging"}
SKIP_SUFFIX = (".tmp", ".part")
DB_SUFFIX = (".sqlite", ".sqlite3", ".db")


def _sha(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _files(root: Path) -> list[Path]:
    out = []
    for dp, dns, fns in os.walk(root):
        dns[:] = sorted(d for d in dns if d not in SKIP_DIRS)
        for f in sorted(fns):
            p = Path(dp) / f
            if f.endswith(SKIP_SUFFIX) or f.endswith(("-wal", "-shm", "-journal")):
                continue
            out.append(p)
    return out


def _sqlite_copy(src: Path, dst: Path) -> str:
    dst.parent.mkdir(parents=True, exist_ok=True)
    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True)
    d = sqlite3.connect(str(dst))
    try:
        s.backup(d)
        res = str(d.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        d.close()
        s.close()
    return res


def _is_sqlite(p: Path) -> bool:
    if not p.name.endswith(DB_SUFFIX):
        return False
    with open(p, "rb") as fh:
        return fh.read(16) == b"SQLite format 3\x00"


def backup(cfg: Any, dest: Path, config_path: Path | None = None) -> dict[str, Any]:
    root = Path(cfg.data_root)
    dest = Path(dest).resolve()
    if root.resolve() in dest.parents or dest == root.resolve():
        raise ValueError("the backup destination must be outside the data root")
    prev = sorted(p for p in dest.glob("*/manifest.json")) if dest.exists() else []
    prev_m = json.loads(prev[-1].read_text()) if prev else None
    snap = dest / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if snap.exists():
        raise FileExistsError(snap)
    (snap / "data").mkdir(parents=True)
    files: dict[str, dict[str, Any]] = {}
    dbs: dict[str, str] = {}
    linked = copied = 0
    for p in _files(root):
        rel = p.relative_to(root).as_posix()
        out = snap / "data" / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        if _is_sqlite(p):
            dbs[rel] = _sqlite_copy(p, out)
            files[rel] = {"size": out.stat().st_size, "sha256": _sha(out), "sqlite": True}
            copied += 1
            continue
        st = p.stat()
        old = (prev_m or {}).get("files", {}).get(rel)
        if old and not old.get("sqlite") and old["size"] == st.st_size and old.get("mtime_ns") == st.st_mtime_ns:
            src_old = prev[-1].parent / "data" / rel
            if src_old.exists() and _sha(p) == old["sha256"]:
                try:
                    os.link(src_old, out)
                    linked += 1
                    files[rel] = {**old}
                    continue
                except OSError:
                    pass
        shutil.copy2(p, out)
        files[rel] = {"size": st.st_size, "mtime_ns": st.st_mtime_ns, "sha256": _sha(out)}
        copied += 1
    cfg_rel = None
    if config_path is not None and Path(config_path).exists():
        cfg_rel = "config/" + Path(config_path).name
        (snap / "config").mkdir()
        shutil.copy2(config_path, snap / cfg_rel)
        files["../" + cfg_rel] = {"size": (snap / cfg_rel).stat().st_size, "sha256": _sha(snap / cfg_rel)}
    m = {"version": 1, "created_utc": datetime.now(timezone.utc).isoformat(), "data_root": str(root), "config": cfg_rel,
         "files": files, "sqlite_integrity": dbs, "copied": copied, "hard_linked": linked,
         "bytes": sum(int(f["size"]) for f in files.values())}  # fmt: skip
    body = json.dumps(m, sort_keys=True, indent=1)
    (snap / "manifest.json").write_text(body)
    (snap / "manifest.sha256").write_text(hashlib.sha256(body.encode()).hexdigest() + "\n")
    bad_db = {k: v for k, v in dbs.items() if v != "ok"}
    return {
        "snapshot": str(snap),
        "files": len(files),
        "copied": copied,
        "hard_linked": linked,
        "bytes": m["bytes"],
        "sqlite": dbs,
        "ok": not bad_db,
    }


def verify_backup(snap: Path) -> dict[str, Any]:
    snap = Path(snap)
    body = (snap / "manifest.json").read_text()
    problems = []
    if hashlib.sha256(body.encode()).hexdigest() != (snap / "manifest.sha256").read_text().strip():
        problems.append("manifest.json does not match manifest.sha256")
    m = json.loads(body)
    for rel, f in m["files"].items():
        p = (snap / "data" / rel) if not rel.startswith("../") else (snap / rel[3:])
        if not p.exists():
            problems.append(f"missing {rel}")
        elif p.stat().st_size != f["size"] or _sha(p) != f["sha256"]:
            problems.append(f"content differs: {rel}")
    for rel in m["sqlite_integrity"]:
        c = sqlite3.connect(f"file:{snap / 'data' / rel}?mode=ro", uri=True)
        try:
            r = c.execute("PRAGMA integrity_check").fetchone()[0]
        except sqlite3.DatabaseError as e:
            r = f"error {e}"
        finally:
            c.close()
        if r != "ok":
            problems.append(f"sqlite {rel}: {r}")
    chains: dict[str, Any] = {}
    from ..execution.audit import AuditBroken, AuditLog

    for a in sorted((snap / "data" / "execution").glob("*/audit.jsonl")):
        lg = AuditLog(a)
        try:
            chains[a.parent.name] = lg.verify()
        except AuditBroken as e:
            problems.append(str(e))
        finally:
            lg.close()
    led = snap / "data" / "research" / "ledger.jsonl"
    if led.exists():
        from ..research import ResearchLedger

        try:
            chains["research_ledger"] = len(ResearchLedger(led.parent).verify())
        except Exception as e:  # noqa: BLE001 - reported as a verification problem
            problems.append(f"research ledger: {e}")
    return {"snapshot": str(snap), "files": len(m["files"]), "chains": chains, "ok": not problems, "problems": problems[:50]}


def restore(snap: Path, target: Path) -> dict[str, Any]:
    snap, target = Path(snap), Path(target)
    v = verify_backup(snap)
    if not v["ok"]:
        raise ValueError(f"snapshot does not verify: {v['problems'][:5]}")
    if target.exists() and any(target.iterdir()):
        raise FileExistsError(
            f"restore target {target} is not empty: restore into a new, empty directory, then switch paths.data_root"
        )
    m = json.loads((snap / "manifest.json").read_text())
    n = 0
    for rel, f in m["files"].items():
        if rel.startswith("../"):
            continue
        out = target / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(snap / "data" / rel, out)
        if _sha(out) != f["sha256"]:
            raise OSError(f"restored file differs from the manifest: {rel}")
        n += 1
    return {"restored_files": n, "target": str(target), "config_in_snapshot": m["config"], "verified": True}
