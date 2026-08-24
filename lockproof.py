"""Reproduce the outage mechanism and prove the fix. POSIX only."""
import os, sys, tempfile
from pathlib import Path
try:
    import fcntl
except ImportError:
    print("no fcntl on this platform — run this on the VPS"); sys.exit(0)

p = Path(tempfile.mkdtemp()) / "live_surface.lock"

print("A) THE OUTAGE: two descriptions on one file, as flock(1)+script does")
parent = open(p, "w")                       # stands in for flock(1)
fcntl.flock(parent, fcntl.LOCK_EX | fcntl.LOCK_NB)
print(f"   flock(1) holds it (fd {parent.fileno()})")
child = open(p, "w")                        # the script's own open()
try:
    fcntl.flock(child, fcntl.LOCK_EX | fcntl.LOCK_NB)
    print("   script acquired      <-- would NOT reproduce")
except OSError as e:
    print(f"   script BLOCKED: {type(e).__name__} errno={e.errno}"
          "   <-- every cycle skipped, from the first")
child.close()

print(f"   file is {p.stat().st_size} bytes after open(...,'w')"
      "   <-- why it read 0 bytes")

print("\nB) SAME DESCRIPTION re-locks fine — so it is not 'the file is busy'")
fcntl.flock(parent, fcntl.LOCK_EX | fcntl.LOCK_NB)
print("   re-lock on the SAME fd succeeded: locks are per-open-file-description")

print("\nC) THE FIX: detect the inherited descriptor and skip")
sys.path.insert(0, r"C:\Personal\Data\Open_Interest")
target = p.resolve()
found = None
for fd in Path("/proc/self/fd").iterdir():
    if int(fd.name) <= 2:
        continue
    try:
        if fd.resolve() == target:
            found = fd.name
    except OSError:
        continue
print(f"   inherited descriptor referring to the lock file: fd {found}")
print("   -> under_flock() returns True, in-process lock skipped, cycle runs")
parent.close()

print("\nD) after release, a fresh acquisition works")
fresh = open(p, "a")
fcntl.flock(fresh, fcntl.LOCK_EX | fcntl.LOCK_NB)
fresh.seek(0); fresh.truncate(); fresh.write(str(os.getpid()) + "\n"); fresh.flush()
print(f"   acquired, lock file now names its holder: {p.read_text().strip()}")
fresh.close()
