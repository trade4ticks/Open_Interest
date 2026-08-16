"""
test_chain_snapshot_writer.py — behaviour of the parquet writer thread.

The writer exists to overlap parquet merges with fetching, which means it owns
data that is fetched but not yet on disk. The failure modes that matters are
therefore about durability and visibility rather than speed:

  * nothing submitted may be lost, and close() must drain before the run
    reports success
  * the queue must be genuinely bounded, and the resulting backpressure must
    be measured rather than silently absorbed
  * a write failure must reach the main thread and stop the run, not be
    swallowed by a daemon thread while fetching continues for hours

write_rows is stubbed, so this needs neither a store nor pyarrow.

Run:  python test_chain_snapshot_writer.py     (exit 1 on any failure)
"""
import sys
import threading
import time

import pandas as pd

import fetch_chain_snapshots as F
from lib.chain_fetch_common import TIMING

PASS, FAIL = [], []


def check(name, got, want):
    ok = got == want
    (PASS if ok else FAIL).append(name)
    print(f"  [{'ok  ' if ok else 'FAIL'}] {name:<50} got={got!r} want={want!r}")


def reset():
    TIMING.writer_secs = 0.0
    TIMING.writer_count = 0
    TIMING.writer_wait = 0.0
    TIMING.writes.clear()


class FakePath:
    def exists(self):
        return True

    def stat(self):
        class S:
            st_size = 5_000_000
        return S()


written = []
_lock = threading.Lock()


def fake_write_rows(ticker, frame, delay=0.0):
    time.sleep(delay)
    with _lock:
        written.append((ticker, len(frame)))
    return {2024: len(frame) * 10}


F.year_path = lambda t, y: FakePath()


def frame(n):
    return pd.DataFrame({"x": range(n)})


print("\n=== 1. every submitted batch reaches disk; close() drains ===")
reset(); written.clear()
F.write_rows = lambda t, f: fake_write_rows(t, f)
w = F._WriterThread(maxsize=2)
w.start()
for i in range(6):
    w.submit("AAPL", frame(i + 1), f"batch{i}")
w.close()
check("batches written", len(written), 6)
check("no rows lost", sum(n for _, n in written), sum(range(1, 7)))
check("writer_count recorded", TIMING.writer_count, 6)
check("TIMING.writes entries", len(TIMING.writes), 6)
check("writes carry (secs,new,total,mb)", len(TIMING.writes[0]), 4)
check("total_rows recorded", TIMING.writes[0][2], 10)

print("\n=== 2. queue is bounded -> backpressure is real and measured ===")
reset(); written.clear()
F.write_rows = lambda t, f: fake_write_rows(t, f, delay=0.15)
w = F._WriterThread(maxsize=1)
w.start()
t0 = time.monotonic()
for i in range(5):
    w.submit("AAPL", frame(1), f"slow{i}")
submit_wall = time.monotonic() - t0
w.close()
check("all slow batches written", len(written), 5)
check("main thread measured a wait", TIMING.writer_wait > 0.05, True)
print(f"         submit wall {submit_wall:.2f}s, writer_wait "
      f"{TIMING.writer_wait:.2f}s, writer_secs {TIMING.writer_secs:.2f}s")

print("\n=== 3. a fast writer hides behind fetching (no wait) ===")
reset(); written.clear()
F.write_rows = lambda t, f: fake_write_rows(t, f, delay=0.01)
w = F._WriterThread(maxsize=2)
w.start()
for i in range(4):
    w.submit("AAPL", frame(1), f"fast{i}")
    time.sleep(0.05)          # stand-in for the next batch's fetching
w.close()
check("no backpressure when writer keeps up", TIMING.writer_wait < 0.02, True)
check("still wrote everything", len(written), 4)

print("\n=== 4. a write failure surfaces, and does NOT hang the run ===")
reset(); written.clear()


def boom(t, f):
    raise OSError("disk full")


F.write_rows = boom
w = F._WriterThread(maxsize=2)
w.start()
raised = None
try:
    for i in range(4):
        w.submit("AAPL", frame(1), f"bad{i}")
        time.sleep(0.05)
except SystemExit as exc:
    raised = str(exc)
if raised is None:
    try:
        w.close()
    except SystemExit as exc:
        raised = str(exc)
check("write failure raises SystemExit", raised is not None, True)
check("message names the failure", "disk full" in (raised or ""), True)
check("message names the store dir", "Store" in (raised or ""), True)

print("\n=== 5. close() is safe when nothing was ever submitted ===")
reset()
F.write_rows = lambda t, f: fake_write_rows(t, f)
w = F._WriterThread(maxsize=2)
w.start()
w.close()
check("empty close() returns", True, True)
check("thread stopped", w.is_alive(), False)

print("\n" + "=" * 60)
print(f"PASSED {len(PASS)} / {len(PASS) + len(FAIL)}")
if FAIL:
    for f_ in FAIL:
        print("  -", f_)
    sys.exit(1)
print("ALL GREEN")
