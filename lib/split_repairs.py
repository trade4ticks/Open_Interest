"""
split_repairs — queue and drive the per-ticker rebuild that a new split requires.

Why this exists
---------------
underlying_ohlc is refreshed on a rolling OHLC_LOOKBACK_DAYS window. yfinance
returns split-ADJUSTED prices (auto_adjust=False only turns off the dividend
adjustment), but only for the rows requested. So the run in which a split first
appears rebases the last ~10 days and leaves everything older in the pre-split
basis: one ticker, two bases, with the step ~7 sessions BEFORE the ex-date
rather than on it.

Every consumer assumes one basis per ticker. build_features multiplies option
strikes by make_split_factors and divides by this table's spot, so every
strike/spot metric came out off by the split ratio on every pre-step row it
recomputed. Observed 2026-09: CVNA (5:1) and FDX (1.24:1) sat in the top
tt_bins bin of oi_weighted_all_div_spot_co from their ex-dates onward on that
artefact alone, and live signals selected them for it. CRWD (4:1) carried the
same error over its refetch band.

What a new split obliges, per ticker, in order
----------------------------------------------
  stage                where it runs       what
  ohlc_refetched_at    either tier         underlying_ohlc from the ticker's
                                           first stored date: one basis again
  features_rebuilt_at  either tier         full build_features, tier BOTH
  bins_rebuilt_at      MORNING only        wf_bins + is_bins for BOTH tiers, and
                                           tt_bins FULL — pre-cutoff rows
                                           changed, so the frozen ruler moved,
                                           which --build-tt-bins-incremental
                                           cannot detect
  paths_rebuilt_at     run_nightly_paths   build_trade_paths --force --tickers:
                       (20:45)             every price column rescales, and the
                                           nightly windowed rebuild only reaches
                                           recent rows. There, not in
                                           run_pipeline, so trade_paths has one
                                           writer that never overlaps a pipeline
                                           run.

tt_bins is full-rebuilt only at MORNING, for the reason run_pipeline documents:
at EVENING the next trading day's row exists with its MORNING columns NULL and
would bin to the (NULL, 0) sentinel. Between detection and that run, the 07:00
incremental build is still correct for every row it writes — it reads the
already-repaired pre-cutoff history — so only older post-cutoff rows wait.

Durability
----------
Detection is written in the SAME transaction as the OHLC upsert that delivers
the split (fetch_ohlc.run). Once that row is stored the split is no longer "new"
by any comparison against underlying_ohlc, so a detection committed separately
could be lost to a crash in between and the repair would never run. The ledger,
not underlying_ohlc, is what "seen before" means.

Each stage stamps its column only on success. A stage that fails or is skipped
stays NULL and the next run retries it; a stage never runs before its
prerequisite (_PREREQ) has completed.

The ledger must be SEEDED before first use (sql/15_split_repairs.sql). An empty
ledger reads every historical split as new.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# yfinance reports "no split" as 0. Anything within this of 1.0 is not a split,
# and a ratio revised by less than this is not a revision. Matches the seed in
# sql/15_split_repairs.sql.
RATIO_TOL = 1e-4

STAGES = ("ohlc_refetched_at", "features_rebuilt_at",
          "bins_rebuilt_at", "paths_rebuilt_at")

# stage -> the stage that must be complete first.
#
# paths has no prerequisite: build_trade_paths reads only the split rows, which
# are in underlying_ohlc from the moment of detection, and its prices come from
# the minute store rather than from the refetched table.
_PREREQ = {
    "ohlc_refetched_at":   None,
    "features_rebuilt_at": "ohlc_refetched_at",
    "bins_rebuilt_at":     "features_rebuilt_at",
    "paths_rebuilt_at":    None,
}

# A refetch that returns fewer rows than this fraction of what is stored is a
# short read, and the rows it missed would keep the old basis — which is the
# defect itself. Not exactly 1.0 because the stored table can hold a row the
# vendor's daily history does not yet (e.g. today's premarket-only open).
MIN_REFETCH_COVERAGE = 0.98


# --- Detection ----------------------------------------------------------------

def is_split(v) -> bool:
    return v is not None and v != 0 and abs(float(v) - 1.0) > RATIO_TOL


def diff_splits(rows: list, known: dict) -> tuple[list, list]:
    """Compare one fetch against the ledger. Pure — no database.

    rows   fetch_ohlc.fetch_one tuples:
           (ticker, date, open, high, low, close, adj_close, volume,
            dividends, splits)
    known  {split_date: ratio} already in the ledger for this ticker

    Returns (new, vanished):
      new       [(date, ratio)] splits the ledger does not hold at this ratio.
                A changed ratio counts: the basis it implies is different.
      vanished  [(date, ratio)] ledger splits on a date this fetch DID return,
                but with no split. The upsert will zero underlying_ohlc.splits,
                load_splits stops seeing the event, and the basis changes again.
    """
    returned = {r[1] for r in rows}
    fetched = {r[1]: float(r[9]) for r in rows if is_split(r[9])}
    new = [(d, x) for d, x in sorted(fetched.items())
           if d not in known
           or abs(x - known[d]) > RATIO_TOL * max(1.0, abs(known[d]))]
    vanished = [(d, x) for d, x in sorted(known.items())
                if d in returned and d not in fetched]
    return new, vanished


def ledger_exists(cur) -> bool:
    cur.execute("SELECT to_regclass('split_repairs')")
    return cur.fetchone()[0] is not None


_QUEUE_SQL = """
INSERT INTO split_repairs (ticker, split_date, ratio)
VALUES (%s, %s, %s)
ON CONFLICT (ticker, split_date) DO UPDATE SET
    ratio               = EXCLUDED.ratio,
    detected_at         = now(),
    ohlc_refetched_at   = NULL,
    features_rebuilt_at = NULL,
    bins_rebuilt_at     = NULL,
    paths_rebuilt_at    = NULL,
    note                = 'ratio revised from ' || split_repairs.ratio
"""


def record_new_splits(cur, ticker: str, rows: list) -> list:
    """Queue a full repair for each split in `rows` the ledger has not seen.

    Uses the CALLER'S cursor and does not commit: it must land in the same
    transaction as the upsert that stores the split rows. Returns the newly
    queued [(date, ratio)].
    """
    if not rows:
        return []
    if not ledger_exists(cur):
        log.error("  %s: split_repairs table missing — split detection is OFF. "
                  "Apply sql/15_split_repairs.sql.", ticker)
        return []

    dates = [r[1] for r in rows]
    cur.execute(
        "SELECT split_date, ratio FROM split_repairs "
        "WHERE ticker = %s AND split_date BETWEEN %s AND %s",
        (ticker, min(dates), max(dates)))
    known = {d: float(x) for d, x in cur.fetchall()}
    new, vanished = diff_splits(rows, known)

    for d, x in vanished:
        # Not auto-repaired: a withdrawn event is rare and ambiguous enough
        # (vendor correction vs vendor hiccup) to want a human first.
        log.error("  %s: ledger split x%g on %s is ABSENT from this fetch. "
                  "underlying_ohlc.splits is being zeroed, which changes the "
                  "price basis every consumer applies. Investigate before the "
                  "next build.", ticker, x, d)
    for d, x in new:
        cur.execute(_QUEUE_SQL, (ticker, d, x))
        log.warning("  %s: NEW split x%g on %s — full repair queued "
                    "(lib/split_repairs.py)", ticker, x, d)
    return new


# --- Ledger state ---------------------------------------------------------------

def pending(conn, stage: str) -> list:
    """Tickers with a split whose `stage` is outstanding and whose prerequisite
    stage is complete. [] when the ledger does not exist."""
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
    prereq = _PREREQ[stage]
    with conn.cursor() as cur:
        if not ledger_exists(cur):
            conn.rollback()
            return []
        sql = f"SELECT DISTINCT ticker FROM split_repairs WHERE {stage} IS NULL"
        if prereq:
            sql += f" AND {prereq} IS NOT NULL"
        cur.execute(sql + " ORDER BY ticker")
        out = [r[0] for r in cur.fetchall()]
    conn.commit()
    return out


def mark_done(conn, tickers: list, stage: str, note: str | None = None) -> None:
    """Stamp `stage` complete for every outstanding split of these tickers.

    Per ticker, not per split: every stage rebuilds the ticker's whole history,
    so one pass satisfies all of its outstanding splits at once. The prerequisite
    is re-checked here so a split queued mid-run can never be stamped by a stage
    that ran before it existed.
    """
    if stage not in STAGES:
        raise ValueError(f"unknown stage {stage!r}; expected one of {STAGES}")
    if not tickers:
        return
    prereq = _PREREQ[stage]
    sql = (f"UPDATE split_repairs SET {stage} = now(), "
           f"note = COALESCE(%s, note) "
           f"WHERE ticker = ANY(%s) AND {stage} IS NULL")
    if prereq:
        sql += f" AND {prereq} IS NOT NULL"
    with conn.cursor() as cur:
        cur.execute(sql, (note, list(tickers)))
    conn.commit()


# --- Stages that run in-process -------------------------------------------------

def refetch_ohlc(conn, ohlc_tickers) -> list:
    """Stage 1: rebase underlying_ohlc across each pending ticker's full history.

    Covers exactly the stored range, first row to last. Earlier than that is not
    this table's history; later is the rolling fetch's job.
    """
    from fetch_ohlc import run as fetch_ohlc_run

    done: list = []
    for t in pending(conn, "ohlc_refetched_at"):
        if t not in ohlc_tickers:
            log.warning("  %s: split repair pending but not an OHLC ticker — "
                        "skipped", t)
            continue
        with conn.cursor() as cur:
            cur.execute("SELECT min(trade_date), max(trade_date), count(*) "
                        "FROM underlying_ohlc WHERE ticker = %s", (t,))
            lo, hi, n_before = cur.fetchone()
        conn.commit()
        if lo is None:
            continue

        log.warning("  %s: split repair 1/4 — refetching underlying_ohlc "
                    "%s → %s (%d rows stored)", t, lo, hi, n_before)
        try:
            n = fetch_ohlc_run(conn, t, lo, hi)
        except Exception as exc:
            conn.rollback()
            log.error("  %s: OHLC refetch failed (%s: %s) — retried next run",
                      t, type(exc).__name__, exc)
            continue
        if n < MIN_REFETCH_COVERAGE * n_before:
            log.error("  %s: refetch returned %d of %d stored rows — a short "
                      "read leaves the old basis on the rows it missed. NOT "
                      "marked; retried next run.", t, n, n_before)
            continue
        mark_done(conn, [t], "ohlc_refetched_at")
        done.append(t)
    return done


def rebuild_features(conn, feature_tickers) -> list:
    """Stage 2: full-history build_features for each pending ticker.

    end is the ticker's last REAL OHLC row, never today. build_for_ticker
    injects a NULL-price placeholder when end is exactly one trading day past
    the last OHLC row, and tier BOTH would then write NULL-derived MORNING
    columns over today's live row. Pinning end to the last stored row makes
    that condition impossible. The windowed per-tier build that follows in the
    same run still writes today's and the next trading day's rows as usual.
    """
    from build_features import build_for_ticker

    done: list = []
    for t in pending(conn, "features_rebuilt_at"):
        if t not in feature_tickers:
            mark_done(conn, [t], "features_rebuilt_at",
                      note="no OI store for this ticker — no features to rebuild")
            continue
        with conn.cursor() as cur:
            cur.execute("SELECT max(trade_date) FROM underlying_ohlc "
                        "WHERE ticker = %s", (t,))
            last = cur.fetchone()[0]
        conn.commit()

        log.warning("  %s: split repair 2/4 — full build_features through %s",
                    t, last)
        try:
            build_for_ticker(conn, t, start=None, end=last, tier="BOTH")
        except Exception as exc:
            conn.rollback()
            log.error("  %s: full build_features failed (%s: %s) — retried "
                      "next run", t, type(exc).__name__, exc)
            continue
        mark_done(conn, [t], "features_rebuilt_at")
        done.append(t)
    return done
