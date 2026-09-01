"""Do pool workers die when the parent is killed with -9?

    python -m scalp.tests.test_worker_reaping

WHY THIS TEST EXISTS. A killed compute run left six orphaned workers holding
5.9 GB of RSS two days later, plus leftovers from an earlier 3-worker run. The
parent was killed with `-9`, which is not catchable: no `atexit` handler, no
`signal.signal`, no `try/finally` and no `ProcessPoolExecutor.__exit__` runs.
Every cleanup mechanism written in Python is bypassed.

`prctl(PR_SET_PDEATHSIG, SIGKILL)` is different in kind. The KERNEL delivers
the signal when the parent dies, so it does not care how the parent died. That
is the claim, and a claim about `-9` deserves a test that actually uses `-9`
rather than a polite `SIGTERM` that would pass either way.

WHAT IT DOES
  1. Spawns a child that starts a ProcessPoolExecutor with the real
     `compute._worker_init`, submits sleeping tasks, prints its workers' PIDs.
  2. `kill -9` on that child — the parent of the pool.
  3. Waits, then checks whether the worker PIDs are still alive.

LINUX ONLY. It skips elsewhere and says so rather than reporting a pass it did
not earn — the mechanism does not exist on Windows or macOS, and a green tick
there would be worse than a skip.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

# Run inside the spawned child: build a pool, report worker PIDs, then idle.
CHILD_SOURCE = """
import os, sys, time
from concurrent.futures import ProcessPoolExecutor
from scalp.compute import _worker_init

def _idle(_):
    time.sleep(600)
    return None

if __name__ == "__main__":
    pool = ProcessPoolExecutor(max_workers=3, initializer=_worker_init)
    futures = [pool.submit(_idle, i) for i in range(3)]
    # Wait until every worker has actually been forked and has run the
    # initializer, otherwise the PIDs below are incomplete.
    deadline = time.time() + 30
    while len(pool._processes) < 3 and time.time() < deadline:
        time.sleep(0.05)
    print(" ".join(str(p) for p in pool._processes), flush=True)
    time.sleep(600)
"""


def alive(pid: int) -> bool:
    """True if the pid exists and is not a zombie."""
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as fh:
            state = fh.read().rsplit(")", 1)[-1].split()[0]
        return state != "Z"
    except OSError:
        return False


def main() -> None:
    if sys.platform != "linux":
        print("SKIPPED — PR_SET_PDEATHSIG is Linux-only, and this is "
              f"{sys.platform}.")
        print("The mechanism does not exist here, so a pass would mean "
              "nothing. Run this on the VPS.")
        raise SystemExit(0)

    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

    print("starting a pool of 3 in a child process...")
    child = subprocess.Popen(
        [sys.executable, "-c", CHILD_SOURCE],
        stdout=subprocess.PIPE, text=True, cwd=repo_root,
    )
    try:
        line = child.stdout.readline().strip()
    except Exception:
        child.kill()
        raise SystemExit("child produced no worker PIDs")
    if not line:
        child.kill()
        raise SystemExit("child produced no worker PIDs")

    workers = [int(p) for p in line.split()]
    print(f"  parent pid : {child.pid}")
    print(f"  worker pids: {workers}")

    before = [w for w in workers if alive(w)]
    print(f"  alive before the kill: {len(before)}/{len(workers)}")
    if len(before) != len(workers):
        child.kill()
        raise SystemExit("workers were not all running — test is invalid")

    print()
    print(f"kill -9 {child.pid}   (the parent, uncatchably)")
    os.kill(child.pid, 9)
    child.wait(timeout=10)

    # The kernel signals the workers as the parent dies, but delivery and
    # teardown are not instantaneous. Poll rather than sleeping a fixed span.
    deadline = time.time() + 15
    survivors = workers
    while time.time() < deadline:
        survivors = [w for w in workers if alive(w)]
        if not survivors:
            break
        time.sleep(0.25)

    print(f"  alive {time.time() - (deadline - 15):.1f}s after: "
          f"{len(survivors)}/{len(workers)}")
    print()

    if survivors:
        for pid in survivors:
            try:
                os.kill(pid, 9)          # do not leak from the test itself
            except OSError:
                pass
        print("FAILED — workers outlived a -9 of their parent:", survivors)
        print()
        print("PR_SET_PDEATHSIG did not fire. The usual cause is that the")
        print("worker was forked from a THREAD that has since exited —")
        print("PDEATHSIG tracks the parent THREAD, not the process — so")
        print("ProcessPoolExecutor's management thread is the thing to check.")
        raise SystemExit(1)

    print("PASSED — every worker died with the parent.")
    print("The kernel delivered SIGKILL on parent death, so the -9 case that")
    print("actually happened is covered, not just the catchable signals.")


if __name__ == "__main__":
    main()
