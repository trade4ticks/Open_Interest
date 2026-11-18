"""
test_spx_intraday.py — run_spx_intraday.py offline.

Cycle logic (subprocess stages stubbed): a no-op before the open, at and after
the close (incl. a 13:00 early close) and on a holiday; inside the session both
builds run for today only, price_5min first, with --brief and the per-stage
timeout; DEGRADED (no OI for today) runs price_5min and skips oi_5min, exit 0;
a failed stage does not stop the other and makes the cycle exit 1.

Then the real compute, on synthetic data for a day in progress (bars printed
through 11:00, a NaN open and a zero open among them):
  * every bar of the day is a row; unprinted bars, the NaN bar and the zero bar
    have NULL metrics; printed bars have values
  * PARITY: each printed bar's row equals the row the next morning's canonical
    rebuild produces once the whole day is in index_ohlc — for both tables

Run:  python test_spx_intraday.py     (exit 1 on any failure)
"""
from __future__ import annotations

import logging
import sys
import tempfile
from datetime import date, datetime, time
from pathlib import Path

import pandas as pd

import lib.spx_features as F
import lib.spx_price as P
import lib.spx_table_b as B
import run_spx_intraday as I

FAILS: list[str] = []


def ok(cond, label: str) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
    if not cond:
        FAILS.append(label)


class Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.lines = []

    def emit(self, record):
        self.lines.append(record.getMessage())


CAP = Capture()
logging.getLogger("spx_intraday").addHandler(CAP)

PRICED = "  spot (--brief): ok 9, zero 69; priced through 10:10; unknown before that: 0\nWROTE 78 rows to t\n"
NONE_YET = "  spot (--brief): no_row 78; priced through none; unknown before that: 0\nWROTE 78 rows to t\n"


def cycle(now, oi=True, rcs=None, out=PRICED):
    calls = []
    rcs = rcs or {}
    CAP.lines.clear()
    I.todays_oi_present = lambda d: (oi, "" if oi else "SPX last OI 2026-11-16")

    def fake_run(argv):
        name = "price_5min" if argv[0].endswith("build_spx_price_5min.py") else "oi_5min"
        calls.append((name, argv))
        rc = rcs.get(name, 0)
        return rc, 1.0, (out if rc == 0 else "hard check FAILED: something\n")
    I._run = fake_run
    return I.run_cycle(now), calls, list(CAP.lines)


def cycle_tests() -> None:
    print("cycle: gating, order, degraded mode, failures, log lines")
    for now, label in ((datetime(2026, 11, 17, 9, 28), "09:28, before the open"),
                       (datetime(2026, 11, 17, 16, 0), "16:00, the close"),
                       (datetime(2026, 11, 17, 16, 3), "16:03, after the close"),
                       (datetime(2026, 11, 27, 13, 3), "13:03 on the 13:00 early close"),
                       (datetime(2026, 11, 26, 10, 3), "Thanksgiving")):
        rc, calls, lines = cycle(now)
        ok(rc == 0 and not calls and len(lines) == 1, f"no-op at {label}, one log line")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 10, 13))
    ok(rc == 0 and [c[0] for c in calls] == ["price_5min", "oi_5min"], "price_5min then oi_5min")
    ok(all(c[1][-5:] == ["--start", "20261117", "--end", "20261117", "--brief"] for c in calls)
       and calls[0][1][0].endswith("build_spx_price_5min.py") and calls[1][1][0].endswith("build_spx_table_b.py"),
       "the existing build scripts, today only, --brief")
    ok(len(lines) == 1 and "price_5min ok" in lines[0] and "(78 rows)" in lines[0]
       and "9 bars priced through 10:10" in lines[0] and "total" in lines[0],
       f"a normal cycle is ONE log line: {lines[0] if lines else None}")
    ok(I.STAGE_TIMEOUT_MIN * 2 <= 3.5, f"two stages capped at {2 * I.STAGE_TIMEOUT_MIN} min of the 5-minute cycle")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 9, 33), out=NONE_YET)
    ok(rc == 0 and len(lines) == 1 and "no printed bars yet" in lines[0] and "expected" in lines[0],
       "09:33: 'no printed bars yet (expected ...)', not an error")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 15, 58))
    ok(rc == 0 and len(calls) == 2, "15:58 (catches the 15:55 bar): both run")
    rc, calls, lines = cycle(datetime(2026, 11, 27, 12, 58))
    ok(rc == 0 and len(calls) == 2, "12:58 on the early close: both run")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 10, 3), oi=False)
    ok(rc == 0 and [c[0] for c in calls] == ["price_5min"] and len(lines) == 2 and "DEGRADED" in lines[0]
       and "SKIPPED" in lines[1], "DEGRADED: price_5min only, exit 0, a warning line plus the cycle line")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 10, 3), rcs={"price_5min": 1})
    ok(rc == 1 and [c[0] for c in calls] == ["price_5min", "oi_5min"], "price_5min fails: oi_5min still runs, exit 1")
    ok(any("its output:" in x and "hard check FAILED" in x for x in lines),
       "a failed stage's full output goes to the log")
    rc, calls, lines = cycle(datetime(2026, 11, 17, 10, 3), rcs={"oi_5min": 124})
    ok(rc == 1, "oi_5min killed by the timeout: exit 1")


# --- the real compute on a day in progress --------------------------------------------------------

NOW = time(11, 0)
NAN_BAR, ZERO_BAR = time(10, 0), time(10, 30)


def in_progress(df: pd.DataFrame, day: date, col: str = "spx_open") -> pd.DataFrame:
    """index_ohlc as it stands at 11:00 on `day`: later bars zero-filled, plus a
    NaN open and a zero open among the printed ones."""
    df = df.copy()
    on = df["trade_date"] == day
    df.loc[on & (df["quote_time"] > NOW), [c for c in df.columns if c.startswith("spx_")]] = 0.0
    df.loc[on & (df["quote_time"] == NAN_BAR), col] = float("nan")
    df.loc[on & (df["quote_time"] == ZERO_BAR), col] = 0.0
    return df


def same_rows(live: pd.DataFrame, canon: pd.DataFrame, cols: list[str]) -> list:
    a = live.set_index("quote_time")
    b = canon.set_index("quote_time")
    bad = []
    for q in a.index:
        if q > NOW or q in (NAN_BAR, ZERO_BAR):
            continue
        for c in cols:
            if not P._same(a.at[q, c], b.at[q, c]):
                bad.append((q, c, a.at[q, c], b.at[q, c]))
    return bad


def price_tests() -> None:
    print("\nspx_price_5min, today in progress")
    import test_spx_price as TP
    full = TP.mock_bars()
    day = TP.SESSIONS[380]
    live_bars = in_progress(full, day)
    res = P.compute_price_5min(day, day, bars=live_bars, seed_from=TP.SEED)
    live = res.frame
    t = live.set_index("quote_time")
    sp, _ = I.parse("  spot (--brief): " + B.brief_spot_line(res.grid) + "\nWROTE 78 rows")
    ok(sp == {"ok": 17, "last": "11:00", "gaps": 2},
       f"--brief line: priced through 11:00, the NaN and zero bars counted as gaps before it: {sp}")
    ok(len(live) == 78, f"{day}: all 78 bars are rows")
    ok(t.loc[t.index > NOW, P.CORE_P5].isna().all().all(), "bars after 11:00 (not printed yet): NULL metrics")
    ok(t.loc[[NAN_BAR, ZERO_BAR], P.CORE_P5].isna().all().all() and t.loc[[NAN_BAR, ZERO_BAR], "spot"].isna().all(),
       "the NaN-open and zero-open bars: NULL spot, NULL metrics")
    printed = [q for q in t.index if q <= NOW and q not in (NAN_BAR, ZERO_BAR)]
    ok(t.loc[printed, "rsi_14"].notna().all(), f"{len(printed)} printed bars carry values")
    # next morning: the day is complete in index_ohlc; the canonical rebuild covers it
    nxt = TP.SESSIONS[381]
    canon = P.compute_price_5min(day, nxt, bars=full, seed_from=TP.SEED).frame
    canon = canon[canon["trade_date"] == day]
    bad = same_rows(live, canon, P.COLUMNS_P5[2:])
    ok(not bad, f"PARITY: every printed bar equals the next morning's rebuild ({len(bad)} differ)" +
       (f": {bad[:3]}" if bad else ""))


def table_b_tests() -> None:
    print("\nspx_oi_5min, today in progress")
    import test_spx_table_b as TB
    no_splits = {r: pd.DataFrame(columns=["trade_date", "splits"]) for r in F.ROOTS}
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        TB.build_store(base)
        kw = dict(oi_store=base / "oi", vol_store=base / "vol", splits=no_splits)
        full = TB.spot_rows()
        day = TB.SESSIONS[-6]                         # 2025-12-23, a regular session
        live_spot = in_progress(full, day)
        res = B.compute_table_b(day, day, spot_source=lambda lo, hi: live_spot[
            (live_spot.trade_date >= lo) & (live_spot.trade_date <= hi)], **kw)
        live = res.frame
        t = live.set_index("quote_time")
        sp, _ = I.parse("  spot (--brief): " + B.brief_spot_line(res.grid))
        ok(sp == {"ok": 17, "last": "11:00", "gaps": 2}, f"--brief line: {sp}")
        ok(len(live) == 78, f"{day}: all 78 bars are rows")
        ok(t.loc[t.index > NOW, B.CORE_B].isna().all().all(), "bars after 11:00: NULL metrics")
        ok(t.loc[[NAN_BAR, ZERO_BAR], B.CORE_B].isna().all().all(), "the NaN-open and zero-open bars: NULL metrics")
        canon = B.compute_table_b(day, TB.SESSIONS[-5], spot_source=lambda lo, hi: full[
            (full.trade_date >= lo) & (full.trade_date <= hi)], **kw).frame
        canon = canon[canon["trade_date"] == day]
        bad = same_rows(live, canon, B.COLUMNS_B[2:])
        ok(not bad, f"PARITY: every printed bar equals the next morning's rebuild ({len(bad)} differ)" +
           (f": {bad[:3]}" if bad else ""))


def main() -> int:
    cycle_tests()
    price_tests()
    table_b_tests()
    print(f"\n{'ALL PASS' if not FAILS else f'{len(FAILS)} FAILURE(S)'}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
