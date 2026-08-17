"""
Shared infrastructure for the chain fetchers.

Extracted from fetch_chain_snapshots.py so fetch_chain_intraday.py reuses the
proven pieces rather than carrying a second copy that drifts. Nothing here is
specific to either fetcher's request shape:

  * run timing accounting (the A / B / C / D summary)
  * in-flight tracking, stall watchdog, occupancy sampler
  * store preflight (path resolution, writability probe, .tmp orphan scan)
  * calendar-day batching
  * interactive prompts
  * vendor date parsing

The fetchers keep what genuinely differs: their request shape, their
projection to the store schema, and their store module.
"""
from __future__ import annotations

import itertools
import logging
import queue
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

from lib.thetadata import (
    SNAPSHOT_TIMING,
    SNAPSHOT_TOTAL_TIMEOUT,
    describe_retry_policy,
    max_connections,
)

log = logging.getLogger(__name__)


# --- File logging -----------------------------------------------------------
# Everything a run produces must survive the terminal: log records AND the
# summary tables. Both go through the same file object so their interleaving
# on disk matches what was on screen.

_LOG_STREAM = None
_LOG_PATH: Path | None = None


def setup_file_logging(script_name: str, log_dir: Path | None = None) -> Path:
    """Tee all logging and summary output to logs/<script>_<timestamp>.log.

    Returns the path. `logs/` is already gitignored.
    """
    global _LOG_STREAM, _LOG_PATH
    d = log_dir or (Path(__file__).resolve().parent.parent / "logs")
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{script_name}_{datetime.now():%Y%m%d_%H%M%S}.log"
    # Line-buffered so a killed run still leaves a usable log.
    _LOG_STREAM = open(path, "w", encoding="utf-8", buffering=1)
    _LOG_PATH = path
    handler = logging.StreamHandler(_LOG_STREAM)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s", "%H:%M:%S"))
    logging.getLogger().addHandler(handler)
    return path


def log_path() -> Path | None:
    return _LOG_PATH


def _emit(line: str = "") -> None:
    """Write a summary line to stdout and to the run log."""
    print(line)
    if _LOG_STREAM is not None:
        try:
            _LOG_STREAM.write(line + "\n")
        except Exception:
            pass


def close_file_logging() -> None:
    global _LOG_STREAM
    if _LOG_STREAM is not None:
        try:
            _LOG_STREAM.flush()
        except Exception:
            pass


# --- Run timing accounting --------------------------------------------------
#
# Two decompositions, because with N concurrent workers they cannot be one:
#
#   (A) A MECE wall-clock timeline of the MAIN thread. At any instant the run
#       is in exactly one of these, so they sum to total wall clock and the
#       remainder is surfaced as "unaccounted".
#   (B) Worker-side aggregates, which OVERLAP each other and (A) by design —
#       4 workers can accumulate 4 seconds of request time per wall second.
#       These give effective concurrency and per-request latency.
#
# Reporting them as one table would be wrong; the summary keeps them apart.

class RunTiming:
    def __init__(self) -> None:
        # (A) main-thread wall clock, mutually exclusive
        self.startup = 0.0
        self.loaded_keys = 0.0
        self.fanout_blocked = 0.0      # main thread waiting on futures
        self.local_compute = 0.0       # projection / result handling
        self.parquet_write = 0.0
        # (B) worker-side, accrued on the MAIN thread from returned elapsed.
        # These count SUCCESSFUL calls only — a task whose future raises never
        # reaches the accrual line, so its time is missing here by
        # construction. Kept for run-to-run diffing; task_* below is the
        # authoritative worker-side measure.
        self.enum_secs = 0.0
        self.enum_count = 0
        self.query_secs = 0.0
        self.query_count = 0
        self.fanout_wall = 0.0         # blocked + local_compute
        # write growth through the run. Entries are
        # (seconds, new_rows, total_rows_after_merge, file_mb) — the last two
        # are what make the read-modify-rewrite cost visible. Recording only
        # new_rows hid it: batch size is roughly constant while the file the
        # batch is merged into grows all year, so the cost driver never
        # appeared in the summary.
        self.writes: list[tuple] = []

        # (E) background writer, when the fetcher hands writes to a thread.
        # Overlaps everything else by design, so it is NOT part of (A).
        self.writer_secs = 0.0         # time the writer thread spent writing
        self.writer_count = 0
        self.writer_wait = 0.0         # main thread blocked on a full queue

        # (B') worker-side, accrued IN THE WORKER by track(), in a finally
        # block, so failed and timed-out tasks are counted too. This is what
        # reconciles against the in-flight sampler.
        self.lock = threading.Lock()
        self.task_secs = 0.0
        self.task_count = 0
        self.enum_task_secs = 0.0
        self.enum_task_count = 0
        self.task_failed_secs = 0.0
        self.task_failed_count = 0
        self.task_failures: dict[str, int] = {}      # exception name -> count
        self.task_failed_secs_by_type: dict[str, float] = {}


TIMING = RunTiming()

# Sampled occupancy, for idle / under-saturation attribution.
SAMPLES: list[tuple[int, bool]] = []     # (in-flight count, local work busy)
_LOCAL_BUSY = False
SAMPLER_STOP = threading.Event()

INFLIGHT: dict[int, tuple[str, float]] = {}
INFLIGHT_LOCK = threading.Lock()
_INFLIGHT_SEQ = itertools.count()
WATCHDOG_STOP = threading.Event()


def set_local_busy(v: bool) -> None:
    """Mark whether the main thread is doing local (non-network) work.

    Read by the sampler to distinguish genuine dead time (nothing in flight
    AND nothing being computed) from local work that legitimately runs with
    the connections idle.
    """
    global _LOCAL_BUSY
    _LOCAL_BUSY = v


def start_sampler(interval: float = 0.1) -> None:
    def _run() -> None:
        while not SAMPLER_STOP.wait(interval):
            with INFLIGHT_LOCK:
                n = len(INFLIGHT)
            SAMPLES.append((n, _LOCAL_BUSY))
    threading.Thread(target=_run, daemon=True, name="sampler").start()


@contextmanager
def track(label: str, kind: str | None = None):
    """Register an in-flight request, and time the whole task body.

    This brackets EXACTLY the interval the sampler counts as "in flight", and
    accrues the duration in a finally block — so a task that raises (timeout,
    rate limit, server error) still contributes its time. The callers'
    main-thread accrual cannot do this: it reads `elapsed` off the future's
    return value, which never exists when the future raised, so failed tasks
    silently contributed zero. That discrepancy is exactly the gap between
    "effective concurrency" and the sampler's mean in-flight.

    `kind` is inferred from the label prefix when not given, so callers that
    have not been updated still classify enumeration correctly.
    """
    if kind is None:
        kind = "enum" if label.startswith("enum ") else "query"
    key = next(_INFLIGHT_SEQ)
    t0 = time.monotonic()
    with INFLIGHT_LOCK:
        INFLIGHT[key] = (label, t0)
    err: str | None = None
    try:
        yield
    except BaseException as exc:
        err = type(exc).__name__
        raise
    finally:
        dt = time.monotonic() - t0
        with INFLIGHT_LOCK:
            INFLIGHT.pop(key, None)
        with TIMING.lock:
            TIMING.task_secs += dt
            TIMING.task_count += 1
            if kind == "enum":
                TIMING.enum_task_secs += dt
                TIMING.enum_task_count += 1
            if err is not None:
                TIMING.task_failed_secs += dt
                TIMING.task_failed_count += 1
                TIMING.task_failures[err] = TIMING.task_failures.get(err, 0) + 1
                TIMING.task_failed_secs_by_type[err] = (
                    TIMING.task_failed_secs_by_type.get(err, 0.0) + dt)


def start_watchdog(interval: float = 30.0, stall_after: float = 90.0,
                   hard_cap: float | None = None) -> None:
    """Log in-flight requests whenever one has been running unusually long.

    Also reports "0 in flight", which is the diagnostic that matters most: it
    proves a stall is LOCAL (parquet merge, sort, loaded_keys) rather than a
    hung vendor request. Those have completely different fixes and are
    otherwise indistinguishable from outside the process.
    """
    def _run() -> None:
        while not WATCHDOG_STOP.wait(interval):
            now = time.monotonic()
            with INFLIGHT_LOCK:
                ages = sorted((now - t0, label) for label, t0 in INFLIGHT.values())
            if not ages:
                log.warning("WATCHDOG: 0 requests in flight — if the run looks "
                            "stopped, it is NOT waiting on the vendor (check "
                            "the parquet merge/sort or loaded_keys)")
                continue
            if ages[-1][0] >= stall_after:
                log.warning("WATCHDOG: %d in flight, oldest %.0fs (hard cap "
                            "%ds): %s", len(ages), ages[-1][0],
                            hard_cap if hard_cap is not None else SNAPSHOT_TOTAL_TIMEOUT,
                            "; ".join(f"{lab} {age:.0f}s"
                                      for age, lab in ages[-4:]))

    threading.Thread(target=_run, daemon=True, name="watchdog").start()


def stop_background_threads() -> None:
    SAMPLER_STOP.set()
    WATCHDOG_STOP.set()


def print_timing_summary(wall_total: float,
                         query_label: str = "point queries",
                         timing: dict | None = None,
                         retry_policy: str | None = None,
                         connections: int | None = None) -> None:
    """Attribute the run's wall clock, then report the concurrent overlay.

    `timing` / `retry_policy` / `connections` default to the ThetaData module's
    values so the existing callers are unchanged. They are injectable so a
    fetcher against a different vendor (lib/polygon.py) renders the SAME
    summary format — the point of the format is diffing runs, which only works
    if every fetcher emits it identically.
    """
    t = TIMING
    vt = timing if timing is not None else SNAPSHOT_TIMING
    policy = retry_policy if retry_policy is not None else describe_retry_policy()
    conns = connections if connections is not None else max_connections()
    accounted = (t.startup + t.loaded_keys + t.fanout_blocked
                 + t.local_compute + t.parquet_write)
    unaccounted = wall_total - accounted

    def pct(x: float) -> float:
        return (100.0 * x / wall_total) if wall_total > 0 else 0.0

    def row(label: str, secs: float) -> None:
        _emit(f"  {label:<38}{secs:>9.1f}s {pct(secs):>6.1f}%")

    _emit("\n" + "=" * 64)
    _emit("TIMING SUMMARY")
    _emit("=" * 64)
    _emit("(A) WALL CLOCK — main-thread timeline, mutually exclusive,")
    _emit("    sums to 100%. This is where the run's real time went.")
    _emit(f"  {'TOTAL':<38}{wall_total:>9.1f}s {100.0:>6.1f}%")
    row("startup (preflight + conn test)", t.startup)
    row("loaded_keys (resumability read)", t.loaded_keys)
    row("fan-out: blocked on network", t.fanout_blocked)
    row("fan-out: local compute (projection)", t.local_compute)
    row("parquet write (merge+sort+rewrite)", t.parquet_write)
    row("unaccounted", unaccounted)

    worker_total = t.enum_secs + t.query_secs
    http  = vt.get("http_seconds", 0.0)
    parse = vt.get("parse_seconds", 0.0)
    mb    = vt.get("http_bytes", 0.0) / 1e6

    _emit("\n(B) WORKER-SIDE — concurrent, OVERLAPS (A) and itself.")
    _emit(f"    {conns} workers accrue up to "
          f"{conns}s of request time per wall second,")
    _emit("    so these deliberately do not sum into (A).")
    _emit(f"  {'total request time (SUCCEEDED only)':<38}{worker_total:>9.1f}s")
    if worker_total > 0:
        _emit(f"  {'  of which HTTP transfer':<38}{http:>9.1f}s "
              f"{100.0 * http / worker_total:>6.1f}%")
        _emit(f"  {'  of which decode (json/rows/frame)':<38}{parse:>9.1f}s "
              f"{100.0 * parse / worker_total:>6.1f}%")
    _emit(f"  {'bytes received':<38}{mb:>9.1f} MB")
    if t.enum_count:
        _emit(f"  enumeration:   {t.enum_count:>6d} calls, {t.enum_secs:>8.1f}s "
              f"total, {t.enum_secs / t.enum_count:>6.2f}s avg")
    if t.query_count:
        _emit(f"  {query_label + ':':<15}{t.query_count:>6d} calls, "
              f"{t.query_secs:>8.1f}s total, "
              f"{t.query_secs / t.query_count:>6.2f}s avg")
    if worker_total > 0 and t.enum_count and t.query_count:
        _emit(f"  enumeration share of request time: "
              f"{100.0 * t.enum_secs / worker_total:.1f}%")

    # --- (B2) authoritative worker-side accounting --------------------------
    # Measured inside the worker in a finally block, so failed and timed-out
    # tasks are included. The lines above count successful calls only.
    sem     = vt.get("sem_wait_seconds", 0.0)
    backoff = vt.get("backoff_seconds", 0.0)
    task    = t.task_secs

    _emit("\n(B2) TASK TIME — every task, including ones that FAILED.")
    _emit("     Measured in the worker, so nothing is lost on the error path.")
    _emit(f"  {'total TASK time (all outcomes)':<38}{task:>9.1f}s")
    _emit(f"  {'  succeeded':<38}{task - t.task_failed_secs:>9.1f}s "
          f"({t.task_count - t.task_failed_count:,} tasks)")
    _emit(f"  {'  FAILED / timed out':<38}{t.task_failed_secs:>9.1f}s "
          f"({t.task_failed_count:,} tasks)")
    if task > 0:
        _emit(f"  {'    (failed share of task time)':<38}"
              f"{100.0 * t.task_failed_secs / task:>9.1f}%")
    _emit("  attribution inside task time:")
    _emit(f"  {'    HTTP transfer':<38}{http:>9.1f}s")
    _emit(f"  {'    decode (json/rows/frame)':<38}{parse:>9.1f}s")
    _emit(f"  {'    backoff sleep (429/474 retries)':<38}{backoff:>9.1f}s")
    _emit(f"  {'    semaphore wait':<38}{sem:>9.1f}s")
    _emit(f"  {'    unattributed inside tasks':<38}"
          f"{task - http - parse - backoff - sem:>9.1f}s")
    _emit(f"  retry policy: {policy}")
    _emit(f"  retries: {vt.get('retry_count', 0.0):.0f} total "
          f"(429 rate-limit: {vt.get('retry_429', 0.0):.0f}, "
          f"474 disconnect: {vt.get('retry_474', 0.0):.0f}, "
          f"5xx: {vt.get('retry_5xx', 0.0):.0f}), "
          f"exhausted: {vt.get('retry_exhausted', 0.0):.0f}")
    if vt.get("pages_followed", 0.0) or vt.get("truncated", 0.0):
        _emit(f"  pagination: {vt.get('pages_followed', 0.0):.0f} next_url page(s) "
              f"followed, {vt.get('truncated', 0.0):.0f} truncated response(s)")
    # Headline for comparing connection settings: what fraction of work was
    # lost, not just how long it took.
    if t.task_count:
        _emit(f"  {'TASK FAILURE RATE':<38}"
              f"{100.0 * t.task_failed_count / t.task_count:>9.2f}% "
              f"({t.task_failed_count:,} of {t.task_count:,})")
    if t.task_failures:
        _emit("  failures by exception type:")
        for name, cnt in sorted(t.task_failures.items(),
                                key=lambda kv: -t.task_failed_secs_by_type.get(kv[0], 0.0)):
            secs = t.task_failed_secs_by_type.get(name, 0.0)
            _emit(f"    {name:<34}{cnt:>6d} x, {secs:>9.1f}s "
                  f"({secs / cnt if cnt else 0:.1f}s avg)")

    _emit("\n(C) CONCURRENCY / SATURATION")
    # Computed from TASK time, which includes failures. The old figure used
    # successful-request time only and therefore under-read whenever tasks
    # failed — that is why it disagreed with the sampler.
    eff = (task / t.fanout_wall) if t.fanout_wall > 0 else 0.0
    eff_old = (worker_total / t.fanout_wall) if t.fanout_wall > 0 else 0.0
    _emit(f"  {'effective concurrency (task time)':<38}{eff:>9.2f} of "
          f"{conns}")
    _emit(f"  {'  legacy figure (succeeded only)':<38}{eff_old:>9.2f} of "
          f"{conns}")
    avail = wall_total * conns
    _emit(f"  {'worker-seconds available (wall x N)':<38}{avail:>9.1f}s")
    _emit(f"  {'worker-seconds accounted (task)':<38}{task:>9.1f}s "
          f"{(100.0 * task / avail) if avail else 0:>6.1f}%")
    _emit(f"  {'worker-seconds unaccounted':<38}{avail - task:>9.1f}s "
          f"{(100.0 * (avail - task) / avail) if avail else 0:>6.1f}%")
    n = len(SAMPLES)
    if n:
        mean_inflight = sum(c for c, _ in SAMPLES) / n
        idle_frac  = sum(1 for c, busy in SAMPLES if c == 0 and not busy) / n
        under_frac = sum(1 for c, _ in SAMPLES
                         if c < conns) / n
        _emit(f"  {'sampled mean in-flight':<38}{mean_inflight:>9.2f}")
        _emit(f"  {'DEAD TIME (0 in flight, no local work)':<38}"
              f"{idle_frac * wall_total:>9.1f}s {idle_frac * 100:>6.1f}%")
        _emit(f"  {f'under-saturated (<{conns} in flight)':<38}"
              f"{under_frac * wall_total:>9.1f}s {under_frac * 100:>6.1f}%")

    if t.writes:
        secs = [w[0] for w in t.writes]
        _emit("\n(D) PARQUET WRITE GROWTH")
        _emit(f"  {'writes':<38}{len(t.writes):>9d}")
        _emit(f"  {'first write':<38}{t.writes[0][0]:>9.1f}s "
              f"({t.writes[0][1]:,} new rows)")
        _emit(f"  {'last write':<38}{t.writes[-1][0]:>9.1f}s "
              f"({t.writes[-1][1]:,} new rows)")
        _emit(f"  {'mean / max write':<38}"
              f"{sum(secs) / len(secs):>9.1f}s / {max(secs):.1f}s")

        # The store's write path is read-modify-rewrite of a whole year file,
        # so cost tracks the FILE, not the batch. Reporting both side by side
        # is what separates "the writes are uneven" from "the writes grow".
        full = [w for w in t.writes if len(w) >= 4 and w[2]]
        if full:
            _emit(f"  {'file rows: first -> last':<38}"
                  f"{full[0][2]:>9,} -> {full[-1][2]:,}")
            _emit(f"  {'file MB:   first -> last':<38}"
                  f"{full[0][3]:>9.1f} -> {full[-1][3]:.1f}")
            slow = max(full, key=lambda w: w[0])
            fast = min(full, key=lambda w: w[0])
            _emit(f"  {'slowest write':<38}{slow[0]:>9.1f}s "
                  f"({slow[1]:,} new rows into {slow[2]:,} / {slow[3]:.1f} MB)")
            _emit(f"  {'fastest write':<38}{fast[0]:>9.1f}s "
                  f"({fast[1]:,} new rows into {fast[2]:,} / {fast[3]:.1f} MB)")
            if fast[2] and slow[2]:
                _emit(f"  {'  size ratio slow/fast':<38}"
                      f"{slow[2] / max(fast[2], 1):>9.1f}x")
                _emit(f"  {'  time ratio slow/fast':<38}"
                      f"{slow[0] / max(fast[0], 1e-9):>9.1f}x")
                _emit("  (the two ratios tracking each other means the spread "
                      "is file")
                _emit("   size under read-modify-rewrite, not contention)")

    if t.writer_count:
        _emit("\n(E) BACKGROUND WRITER — overlaps (A), so not part of it.")
        _emit(f"  {'writes off the main thread':<38}{t.writer_count:>9d}")
        _emit(f"  {'writer thread busy':<38}{t.writer_secs:>9.1f}s "
              f"{pct(t.writer_secs):>6.1f}%")
        _emit(f"  {'main thread blocked on queue':<38}{t.writer_wait:>9.1f}s "
              f"{pct(t.writer_wait):>6.1f}%")
        hidden = t.writer_secs - t.writer_wait
        _emit(f"  {'write time hidden behind fetching':<38}{hidden:>9.1f}s "
              f"{pct(hidden):>6.1f}%")
        if t.writer_wait > 0.25 * t.writer_secs:
            _emit("  NOTE: the writer is the bottleneck for a meaningful share "
                  "of the run;")
            _emit("        raising --write-queue only defers it, the fix is a "
                  "cheaper write.")
    _emit("=" * 64)


# --- Background parquet writer ----------------------------------------------

class ParquetWriterThread(threading.Thread):
    """Serialises parquet writes off the main thread.

    Both fetchers had the fan-out and the write sharing one thread, so a
    sizeable share of every run had zero requests in flight while a year file
    was merged and rewritten. Handing the frame over lets the next batch's
    requests start immediately; network time and write time overlap instead of
    summing.

    Exactly ONE writer, always. The stores' write path is read-modify-rewrite
    of a whole year file, so two threads writing the same (ticker, year) would
    interleave read and rename and lose rows. Serialising here keeps that
    contract while still taking it off the critical path.

    The queue is bounded. An unbounded one would let a slow writer accumulate
    batch frames until the process died of memory rather than of anything
    diagnosable; blocking the producer instead makes backpressure visible as
    TIMING.writer_wait, which is the number that says whether the writer has
    become the bottleneck. Sizing matters more for the intraday store, where a
    single batch frame is a full session of 5-minute bars.

    write_rows / year_path are injected rather than imported so the two stores
    can share this without it knowing which one it is writing to.
    """

    def __init__(self, write_rows, year_path, store_dir, maxsize: int = 2):
        super().__init__(daemon=True, name="parquet-writer")
        self._write_rows = write_rows
        self._year_path = year_path
        self._store_dir = store_dir
        self.q: "queue.Queue" = queue.Queue(maxsize=maxsize)
        self.error: BaseException | None = None
        self.error_ctx: str = ""

    def submit(self, ticker: str, frame, ctx: str) -> None:
        """Block until the writer has room, then hand off. Raises whatever the
        writer already failed with, so a write failure stops the run promptly
        rather than after every remaining ticker has been fetched for nothing."""
        self._raise_if_failed()
        t0 = time.monotonic()
        self.q.put((ticker, frame, ctx))
        waited = time.monotonic() - t0
        with TIMING.lock:
            TIMING.writer_wait += waited

    def _raise_if_failed(self) -> None:
        if self.error is not None:
            raise SystemExit(
                f"FATAL: parquet write failed ({self.error_ctx}): {self.error}\n"
                f"Store dir: {self._store_dir}\n"
                "Aborting rather than continuing to fetch with nothing stored."
            )

    def run(self) -> None:
        while True:
            item = self.q.get()
            try:
                if item is None:
                    return
                ticker, frame, ctx = item
                t0 = time.monotonic()
                try:
                    by_year = self._write_rows(ticker, frame)
                except BaseException as exc:            # noqa: BLE001
                    # A write failure is systemic (permissions, schema, disk),
                    # not batch-specific. Record it; the main thread raises.
                    self.error, self.error_ctx = exc, ctx
                    log.error("  %s: PARQUET WRITE FAILED — %s", ctx, exc,
                              exc_info=True)
                    continue
                secs = time.monotonic() - t0

                if not by_year:
                    log.error("  %s: write_rows accepted %d rows but wrote no "
                              "year file — every row had an unusable "
                              "trade_date", ctx, len(frame))
                    continue

                total_rows = 0
                total_mb = 0.0
                for y, n in sorted(by_year.items()):
                    p = self._year_path(ticker, y)
                    mb = (p.stat().st_size / 1e6) if p.exists() else 0.0
                    total_rows += n
                    total_mb += mb
                    log.info("    WROTE %s -> %d rows total, %.1f MB", p, n, mb)
                with TIMING.lock:
                    TIMING.writer_secs += secs
                    TIMING.writer_count += 1
                    TIMING.writes.append((secs, len(frame), total_rows, total_mb))
                log.info("    write took %.1fs for %d new rows into %d rows / "
                         "%.1f MB (fetching continued throughout)",
                         secs, len(frame), total_rows, total_mb)
            finally:
                self.q.task_done()

    def close(self) -> None:
        """Drain, stop, and surface any failure."""
        self.q.put(None)
        self.join()
        self._raise_if_failed()


# --- Store preflight --------------------------------------------------------

def preflight_store(store_dir: Path, sibling_dir: Path) -> None:
    """Resolve the store path, prove it is writable, report .tmp orphans.

    Runs before any fetching so a misconfigured or unwritable store fails in
    seconds rather than after an hour of successful fetching that stores
    nothing.  Creating the directory here also means its absence after a run
    is no longer ambiguous: the folder always exists, so an empty one means
    "no rows survived", not "the path was wrong".
    """
    _emit(f"Store:    {store_dir}")
    _emit(f"Sibling:  {sibling_dir}  (exists={sibling_dir.exists()})")
    log.info("store dir resolves to %s", store_dir)

    try:
        store_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise SystemExit(f"FATAL: cannot create {store_dir}: {exc}")

    probe = store_dir / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except Exception as exc:
        raise SystemExit(
            f"FATAL: {store_dir} is not writable by this process: {exc}"
        )
    log.info("  write permission OK")

    orphans = sorted(store_dir.glob("*/*.parquet.tmp"))
    if orphans:
        log.error("  %d orphaned .parquet.tmp file(s) — a previous write died "
                  "between write and rename. Inspect before trusting the "
                  "store: %s", len(orphans),
                  ", ".join(str(p) for p in orphans[:5]))
    else:
        log.info("  no orphaned .tmp files")


# --- Batching ---------------------------------------------------------------

def chunk_range(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    """Split [start, end] into inclusive calendar-day windows of <= max_days.

    These are WRITE batches, not request windows — every request the fetchers
    issue covers a single session.  A batch with no trading days is harmless:
    it yields no sessions and is skipped.
    """
    if max_days < 1:
        raise ValueError("max_days must be >= 1")
    out: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        w_end = min(cur + timedelta(days=max_days - 1), end)
        out.append((cur, w_end))
        cur = w_end + timedelta(days=1)
    return out


# --- Prompts ----------------------------------------------------------------

def prompt_tickers(fallback) -> list[str]:
    raw = input(
        "Tickers (comma-separated; blank = all tickers in OI store): "
    ).strip()
    if raw:
        return [t.strip().upper() for t in raw.split(",") if t.strip()]
    out = fallback()
    if not out:
        raise SystemExit(
            "No tickers entered and OI store is empty — please specify."
        )
    return out


def prompt_date(label: str, default: date | None = None) -> date:
    """Prompt for a YYYYMMDD date.

    `default` is optional and backwards-compatible: callers that pass none get
    the original behaviour (a date must be typed). When given, it is shown in
    the prompt and accepted on a blank line — useful where a fetcher has an
    obvious canonical value, such as a backfill that always starts at the same
    history epoch.
    """
    suffix = f" [{default:%Y%m%d}]" if default is not None else ""
    while True:
        raw = input(f"{label} (YYYYMMDD){suffix}: ").strip()
        if not raw and default is not None:
            return default
        try:
            return datetime.strptime(raw, "%Y%m%d").date()
        except ValueError:
            _emit("  Use YYYYMMDD (e.g. 20240102)")


# --- Vendor date parsing ----------------------------------------------------

def to_date_series(s):
    """Parse a vendor date column that may be 'YYYY-MM-DD', 'YYYYMMDD', or the
    integer 20241104.

    Stringify first — pd.to_datetime on a raw int reads it as a nanosecond
    epoch.  Numeric columns route through Int64 rather than a plain
    astype("string"): if a single row has a null date the whole column comes
    back as float64, and str(20241104.0) is "20241104.0", which parses to NaT
    and would silently drop EVERY row in the response.
    """
    import pandas as pd
    if pd.api.types.is_integer_dtype(s) or pd.api.types.is_float_dtype(s):
        s = s.astype("Int64")
    return pd.to_datetime(s.astype("string"), errors="coerce")
