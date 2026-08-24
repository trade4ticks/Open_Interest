"""Reproduce the run_cycle deadlock and prove the interleaved loop fixes it.

Each variant runs in a DAEMON thread: the old structure wedges its executor so
badly that even shutdown blocks, so the harness must be able to walk away.
"""
import threading, time
from concurrent.futures import (FIRST_COMPLETED, ThreadPoolExecutor,
                                as_completed, wait)

N, CONN, INFLIGHT = 121, 3, 12
WORK = 0.002

def run(fn, budget):
    box = {}
    t = threading.Thread(target=lambda: box.update(fn()), daemon=True)
    t0 = time.monotonic(); t.start(); t.join(budget)
    return box, t.is_alive(), time.monotonic() - t0

def old():
    slots = threading.Semaphore(INFLIGHT); prog = {"fetched": 0, "fitted": 0}
    def guarded(tk):
        slots.acquire(); time.sleep(WORK); return tk
    fp = ThreadPoolExecutor(max_workers=CONN)
    fitp = ThreadPoolExecutor(max_workers=2)
    ff = {fp.submit(guarded, i): i for i in range(N)}
    gf = {}
    for fut in as_completed(ff):                 # loop 1: ALL fetches
        fut.result(); prog["fetched"] += 1
        gf[fitp.submit(time.sleep, WORK)] = 1
    for fut in as_completed(gf):                 # loop 2: releases live HERE
        slots.release(); prog["fitted"] += 1
    return prog

def new():
    slots = threading.Semaphore(INFLIGHT); prog = {"fetched": 0, "fitted": 0}
    def guarded(tk):
        slots.acquire(); time.sleep(WORK); return tk
    with ThreadPoolExecutor(max_workers=CONN) as fp, \
         ThreadPoolExecutor(max_workers=2) as fitp:
        pending = {fp.submit(guarded, i): ("fetch", i) for i in range(N)}
        while pending:
            ready, _ = wait(pending, return_when=FIRST_COMPLETED, timeout=2.0)
            if not ready:
                break
            for fut in ready:
                kind, tk = pending.pop(fut)
                if kind == "fetch":
                    fut.result(); prog["fetched"] += 1
                    pending[fitp.submit(time.sleep, WORK)] = ("fit", tk)
                else:
                    slots.release(); prog["fitted"] += 1
    return prog

for label, fn, budget in (("OLD (two loops)", old, 8.0),
                          ("NEW (interleaved)", new, 45.0)):
    prog, alive, el = run(fn, budget)
    status = "DEADLOCK" if alive else "completed"
    f, g = prog.get("fetched", 0), prog.get("fitted", 0)
    print(f"  {label:<20}{status:<11}fetched {f:>3}/{N}  fitted {g:>3}/{N}"
          f"  after {el:.1f}s")
    if alive:
        print(f"       wedged at fetch {f} — max_inflight is {INFLIGHT}, so the "
              f"{INFLIGHT+1}th blocks on a release loop 2 has not started")
