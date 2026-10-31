"""Apply a code update archive on a mount where files cannot be deleted: extract to a temp dir, then
os.replace() each file into place (atomic per file, never unlinks).   python3 scripts/apply_update.py update.tgz"""
import os, shutil, sys, tarfile, tempfile
tmp = tempfile.mkdtemp(dir=os.path.expanduser("~"))
tarfile.open(sys.argv[1]).extractall(tmp)
for root, _, files in os.walk(tmp):
    for f in files:
        src = os.path.join(root, f); rel = os.path.relpath(src, tmp); dst = os.path.join(".", rel)
        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        shutil.copyfile(src, dst + ".new"); os.replace(dst + ".new", dst)
        if dst.endswith(".sh"): os.chmod(dst, 0o755)
        print("updated", rel)
