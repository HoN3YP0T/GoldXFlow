"""Consistent SQLite snapshot copy between a local working DB and a persistent folder.

  python3 scripts/snapshot.py save    <local.sqlite> <persistent.sqlite>
  python3 scripts/snapshot.py restore <persistent.sqlite> <local.sqlite>

save uses SQLite's online backup API into a uniquely named temp file next to the target, then os.replace(),
so the persistent copy is always a complete database (never half-written), and never needs file deletion.
A stale '<target>-journal' beside the target would be replayed as a hot journal by SQLite, so it is moved
into data/_to_delete/ first.
"""
import os, sqlite3, sys, time
from pathlib import Path

def _aside(p: Path):
    if p.exists():
        root = next((x for x in p.parents if x.name == "data"), p.parent)
        (root / "_to_delete").mkdir(exist_ok=True)
        os.replace(p, root / "_to_delete" / f"{time.strftime('%Y%m%dT%H%M%S')}-{p.name}")

def save(src: str, dst: str):
    dst = Path(dst)
    for old in dst.parent.glob(dst.name + ".snap-*"): _aside(old)      # half-written copies from interrupted saves
    tmp = dst.with_name(f"{dst.name}.snap-{os.getpid()}-{int(time.time())}")
    s = sqlite3.connect(src); d = sqlite3.connect(str(tmp))
    d.execute("PRAGMA journal_mode=OFF")
    s.backup(d); d.close(); s.close()
    for j in (dst.with_name(dst.name + "-journal"), dst.with_name(dst.name + "-wal"), dst.with_name(dst.name + "-shm")):
        _aside(j)
    os.replace(tmp, dst)
    print(f"snapshot saved -> {dst} ({dst.stat().st_size / 1e6:.1f} MB)")

def restore(src: str, dst: str):
    Path(dst).parent.mkdir(parents=True, exist_ok=True)
    s = sqlite3.connect(f"file:{src}?mode=ro", uri=True); d = sqlite3.connect(dst)
    s.backup(d); d.close(); s.close()
    print(f"restored {src} -> {dst}")

if __name__ == "__main__":
    {"save": save, "restore": restore}[sys.argv[1]](sys.argv[2], sys.argv[3])
