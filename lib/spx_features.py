"""
lib/spx_features.py — table A (daily SPX/SPXW OI + volume metrics) compute.

Pure compute: reads the two raw stores, returns a DataFrame, writes nothing.
build_spx_features.py writes it; verify_spx_features.py and
test_spx_features.py check it. The build specification is the authority for
every definition here.

ONE BOOK, TWO DIRECTORIES
-------------------------
{store}/SPX/{year}.parquet and {store}/SPXW/{year}.parquet are unioned with a
literal `root` column before anything aggregates. Every metric is over the
combined book except pct_oi_spxw / pct_vol_spxw.

TIMING (spec §2)
----------------
Row T = OI labelled trade_date T  +  volume of session T-1.

The volume store's `trade_date` is the session the trades happened in. It is
renamed `vol_session` the moment it is read and never exposed under its own
name; feature_date = lib.market_hours.next_trading_day(vol_session) is the row
it feeds, and the only join to OI is  spine.trade_date = vol.feature_date.
No query in this module sees two columns named trade_date.

check_table_a() fails the build unless, for every row carrying volume, exactly
one volume session contributed, it is strictly earlier than trade_date, and it
is the immediately preceding NYSE session.

VOLUME DTE IS MEASURED FROM THE VOLUME SESSION
----------------------------------------------
dte(volume) = expiration - vol_session: a contract's days to expiry on the day
it traded, as build_features.VOL_FEATURES_SQL measures it. front_exp for the
volume side is MIN(expiration WHERE dte >= 0) of that session, both roots, so
pct_vol_front_expiry is the share of T-1's volume in T-1's own front expiry
(0DTE wherever one listed). Measured from T instead, every 0DTE trade on T-1
would carry dte = -1 and drop out of every DTE bucket.

So pct_vol_front_expiry and pct_oi_front_expiry are NOT a symmetric pair:
different reference days, different expiries. See CATALOG_DESCRIPTIONS.

OI dte = expiration - trade_date, as in the factor project.

SHARED SQL
----------
The OI-side aggregates come from build_features.OI_FEATURES_SQL, imported and
run on the unioned book with an empty `ohlc` and `next_monthly_df`, so every
spot-dependent column it emits is NULL and is ignored. Columns taken from it
are listed in _FACTOR_OI_COLS. A change made there for the factor project
changes SPX too; verify_spx_features.py --regress is what makes that loud.

JOINING THE STORES
------------------
No metric needs a contract-level join: each side is aggregated on its own and
the two meet at the day level on the trade_date spine. A contract with OI and
no volume row contributes 0 volume; one with volume and no OI row contributes
0 OI. Neither side can drop the other's contracts.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from build_features import OI_FEATURES_SQL
from lib.market_hours import get_trading_days, next_trading_day
from lib.split_factors import make_split_factors

ROOTS = ("SPX", "SPXW")
TABLE_A = "spx_oi_daily"

Z_WINDOW = 60
# z on the level needs 59 sessions before the first output row, and
# net_new_oi_div_vol's level needs one more (its d1_total_oi_change).
LEAD_IN_SESSIONS = Z_WINDOW

# --- Column registry ---------------------------------------------------------

CORE_A = [
    "total_oi",
    "oi_put_call_ratio",
    "oi_put_call_ratio_0_30d",
    "vol_put_call_ratio",
    "weighted_avg_dte_oi",
    "weighted_avg_dte_vol",
    "pct_oi_front_expiry",
    "pct_vol_front_expiry",
    "pct_oi_0_30d",
    "pct_vol_0_30d",
    "pct_oi_top5_strikes",
    "pct_vol_top5_strikes",
    "pct_oi_spxw",
    "pct_vol_spxw",
    "vol_oi_ratio_all",
    "vol_oi_ratio_call",
    "vol_oi_ratio_put",
    "net_new_oi_div_vol",
]

INGREDIENTS_A = [
    "call_oi", "put_oi", "total_vol", "call_vol", "put_vol",
    "oi_weighted_all", "oi_weighted_call", "oi_weighted_put", "oi_weighted_all_0_30d",
    "vol_weighted_all", "vol_weighted_call", "vol_weighted_put",
    "max_oi_strike_put",
]

COUNT_COLS = {"total_oi", "call_oi", "put_oi", "total_vol", "call_vol", "put_vol"}


def derivative_names(m: str) -> list[str]:
    if m == "total_oi":
        return ["d1_total_oi_pct_change", "d5_total_oi_pct_change", "zscore_total_oi_3m"]
    return [f"d1_{m}_change", f"d5_{m}_change", f"zscore_{m}_3m"]


# Raw levels stored in the table but NOT surfaced in the catalog: their values
# do not overlap enough year over year for a fixed threshold to mean the same
# thing across 2020-2026 (measured by the user in Postgres). Their d1, d5 and
# z-score columns stay surfaced; the levels stay in the table to interpret a
# z-score hit. Reversible: remove a name here to surface it again.
UNSURFACED_LEVELS_A = [
    "pct_vol_front_expiry", "pct_oi_top5_strikes",
    "vol_oi_ratio_call", "vol_oi_ratio_all", "vol_oi_ratio_put",
    "pct_vol_spxw", "pct_vol_0_30d",
    "weighted_avg_dte_oi", "weighted_avg_dte_vol",
]
SURFACED_LEVELS_A = [m for m in CORE_A if m != "total_oi" and m not in UNSURFACED_LEVELS_A]
DERIVATIVES_A = [d for m in CORE_A for d in derivative_names(m)]
SURFACED_A = SURFACED_LEVELS_A + DERIVATIVES_A
STORED_ONLY_A = ["total_oi", *UNSURFACED_LEVELS_A, *INGREDIENTS_A]
META_A = ["vol_session"]
COLUMNS_A = ["trade_date", *META_A, *CORE_A, *DERIVATIVES_A, *INGREDIENTS_A]
# Carried on the compute result for the checks; not written.
DIAGNOSTICS_A = ["n_vol_sessions", "vol_roots", "vol_rows_neg_dte", "oi_roots", "oi_rows_neg_dte"]

assert len(CORE_A) == 18 and len(INGREDIENTS_A) == 13
assert set(UNSURFACED_LEVELS_A) <= set(CORE_A) and len(UNSURFACED_LEVELS_A) == 9
assert len(SURFACED_A) == 62 and len(STORED_ONLY_A) == 23
assert sorted(SURFACED_A + STORED_ONLY_A) == sorted(COLUMNS_A[2:])
assert not any(c.endswith(("_pc", "_co")) for c in COLUMNS_A)

# Catalog descriptions (table C, phase 4, reads these). Seeded with the ones
# whose meaning is not what the name suggests; phase 4 fills in the rest.
CATALOG_DESCRIPTIONS = {
    "pct_oi_front_expiry": (
        "Share of the standing open-interest book (as of T-1's close, published on T) that sits "
        "in the nearest expiration on or after T, both roots. Measures the book, not trading. "
        "NOT the OI counterpart of pct_vol_front_expiry: that one is the share of T-1's trading "
        "in T-1's own nearest expiry, a different expiry and a different question."
    ),
    "pct_vol_front_expiry": (
        "Share of session T-1's traded volume in T-1's own nearest expiration (DTE measured from "
        "the volume session, so a contract expiring on T-1 counts as 0DTE). Since roughly 2023, "
        "with SPXW expiries every weekday, that nearest expiry is T-1 itself and this is "
        "effectively the 0DTE share of volume. Earlier its meaning shifts: in 2020, with "
        "Mon/Wed/Fri expirations, the nearest expiry was often the next day, so on those "
        "sessions it measures 1DTE trading. Not the volume counterpart of pct_oi_front_expiry, "
        "which is the standing book's share in T's nearest expiry. Compare across years only "
        "through its z-score."
    ),
    "d1_total_oi_pct_change": (
        "Percentage change in total open interest from T-2's close to T-1's close: OI labelled T "
        "reflects T-1's close, so this is one session earlier than the name suggests. "
        "Lookahead-free."
    ),
    "d5_total_oi_pct_change": (
        "Percentage change in total open interest from T-6's close to T-1's close (OI labelled T "
        "reflects T-1's close). Lookahead-free."
    ),
}

_NOT_SURFACED_NOTE = (
    "Raw level not surfaced: its values do not overlap enough year over year for a fixed "
    "threshold to mean the same thing across 2020-2026. Use its d1, d5 and z-score columns, "
    "which are surfaced; the level is stored to interpret a z-score hit."
)
_ZERO_OVERLAP_NOTE = " No 2026 value falls in the range of values 2020 produced: zero overlap."
for _m in UNSURFACED_LEVELS_A:
    _note = _NOT_SURFACED_NOTE + (_ZERO_OVERLAP_NOTE if _m in ("pct_vol_front_expiry",
                                                                "pct_oi_top5_strikes") else "")
    CATALOG_DESCRIPTIONS[_m] = (CATALOG_DESCRIPTIONS[_m] + " " + _note
                                if _m in CATALOG_DESCRIPTIONS else _note)

# Columns read from build_features.OI_FEATURES_SQL, renamed to the spec's names.
_FACTOR_OI_COLS = {
    "total_oi": "total_oi",
    "call_oi": "call_oi",
    "put_oi": "put_oi",
    "put_call_oi_ratio": "oi_put_call_ratio",
    "weighted_avg_dte": "weighted_avg_dte_oi",
    "pct_oi_in_front_expiry": "pct_oi_front_expiry",
    "pct_oi_0_30d": "pct_oi_0_30d",
    "top5_strikes_pct_total_oi": "pct_oi_top5_strikes",
    "oi_weighted_all": "oi_weighted_all",
    "oi_weighted_call": "oi_weighted_call",
    "oi_weighted_put": "oi_weighted_put",
    "oi_weighted_all_0_30d": "oi_weighted_all_0_30d",
    "max_oi_strike_put": "max_oi_strike_put",
}


# --- SQL ---------------------------------------------------------------------

OI_EXTRA_SQL = """
SELECT trade_date,
       SUM(CASE WHEN option_type = 'P' AND dte BETWEEN 0 AND 30 THEN open_interest ELSE 0 END)::DOUBLE
         / NULLIF(SUM(CASE WHEN option_type = 'C' AND dte BETWEEN 0 AND 30 THEN open_interest ELSE 0 END), 0)
                                                               AS oi_put_call_ratio_0_30d,
       SUM(CASE WHEN root = 'SPXW' THEN open_interest ELSE 0 END)::DOUBLE
         / NULLIF(SUM(open_interest), 0)                       AS pct_oi_spxw,
       COUNT(DISTINCT root)                                    AS oi_roots,
       COUNT(*) FILTER (WHERE dte < 0)                         AS oi_rows_neg_dte
FROM (SELECT *, (expiration - trade_date)::INTEGER AS dte FROM oi)
GROUP BY trade_date
"""

# Keyed by feature_date (the row it feeds); vol_session is carried through
# for the lookahead check and stored on the row.
VOL_AGG_SQL = """
WITH v AS (
    SELECT root, vol_session, feature_date, expiration, strike, option_type, volume,
           (expiration - vol_session)::INTEGER AS dte
    FROM vol
),
per AS (
    SELECT feature_date,
           MAX(vol_session)                                                   AS vol_session,
           COUNT(DISTINCT vol_session)                                        AS n_vol_sessions,
           COUNT(DISTINCT root)                                               AS vol_roots,
           COUNT(*) FILTER (WHERE dte < 0)                                    AS vol_rows_neg_dte,
           SUM(volume)::BIGINT                                                AS total_vol,
           SUM(CASE WHEN option_type = 'C' THEN volume ELSE 0 END)::BIGINT    AS call_vol,
           SUM(CASE WHEN option_type = 'P' THEN volume ELSE 0 END)::BIGINT    AS put_vol,
           SUM(CASE WHEN dte BETWEEN 0 AND 30 THEN volume ELSE 0 END)::BIGINT AS vol_0_30d,
           SUM(CASE WHEN root = 'SPXW' THEN volume ELSE 0 END)::BIGINT        AS vol_spxw,
           SUM(dte * volume)::DOUBLE / NULLIF(SUM(volume), 0)                 AS weighted_avg_dte_vol,
           SUM(strike * volume)::DOUBLE / NULLIF(SUM(volume), 0)              AS vol_weighted_all,
           SUM(CASE WHEN option_type = 'C' THEN strike * volume ELSE 0 END)::DOUBLE
             / NULLIF(SUM(CASE WHEN option_type = 'C' THEN volume ELSE 0 END), 0) AS vol_weighted_call,
           SUM(CASE WHEN option_type = 'P' THEN strike * volume ELSE 0 END)::DOUBLE
             / NULLIF(SUM(CASE WHEN option_type = 'P' THEN volume ELSE 0 END), 0) AS vol_weighted_put
    FROM v
    GROUP BY feature_date
),
front AS (
    SELECT feature_date, MIN(expiration) AS front_exp
    FROM v WHERE dte >= 0
    GROUP BY feature_date
),
front_vol AS (
    SELECT v.feature_date, SUM(v.volume)::BIGINT AS front_vol
    FROM v JOIN front f ON f.feature_date = v.feature_date AND f.front_exp = v.expiration
    GROUP BY v.feature_date
),
strike_vol AS (
    SELECT feature_date, strike, SUM(volume) AS sv
    FROM v GROUP BY feature_date, strike
),
top5 AS (
    SELECT feature_date, SUM(sv)::BIGINT AS top5_vol
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY feature_date ORDER BY sv DESC, strike) AS rk
          FROM strike_vol)
    WHERE rk <= 5
    GROUP BY feature_date
)
SELECT p.*, fv.front_vol, t.top5_vol
FROM per p
LEFT JOIN front_vol fv USING (feature_date)
LEFT JOIN top5      t  USING (feature_date)
"""

LEVELS_SQL = """
SELECT s.trade_date,
       v.vol_session, v.n_vol_sessions, v.vol_roots, v.vol_rows_neg_dte,
       o.oi_roots, o.oi_rows_neg_dte,
       f.total_oi,
       f.oi_put_call_ratio,
       o.oi_put_call_ratio_0_30d,
       v.put_vol::DOUBLE / NULLIF(v.call_vol, 0)        AS vol_put_call_ratio,
       f.weighted_avg_dte_oi,
       v.weighted_avg_dte_vol,
       f.pct_oi_front_expiry,
       v.front_vol::DOUBLE / NULLIF(v.total_vol, 0)     AS pct_vol_front_expiry,
       f.pct_oi_0_30d,
       v.vol_0_30d::DOUBLE / NULLIF(v.total_vol, 0)     AS pct_vol_0_30d,
       f.pct_oi_top5_strikes,
       v.top5_vol::DOUBLE / NULLIF(v.total_vol, 0)      AS pct_vol_top5_strikes,
       o.pct_oi_spxw,
       v.vol_spxw::DOUBLE / NULLIF(v.total_vol, 0)      AS pct_vol_spxw,
       v.total_vol::DOUBLE / NULLIF(f.total_oi, 0)      AS vol_oi_ratio_all,
       v.call_vol::DOUBLE  / NULLIF(f.call_oi, 0)       AS vol_oi_ratio_call,
       v.put_vol::DOUBLE   / NULLIF(f.put_oi, 0)        AS vol_oi_ratio_put,
       -- LAG over the calendar spine: a missing session yields NULL, never a
       -- change spanning two sessions.
       (f.total_oi - LAG(f.total_oi, 1) OVER (ORDER BY s.trade_date))::DOUBLE
         / NULLIF(v.total_vol, 0)                       AS net_new_oi_div_vol,
       f.call_oi, f.put_oi,
       v.total_vol, v.call_vol, v.put_vol,
       f.oi_weighted_all, f.oi_weighted_call, f.oi_weighted_put, f.oi_weighted_all_0_30d,
       v.vol_weighted_all, v.vol_weighted_call, v.vol_weighted_put,
       f.max_oi_strike_put
FROM spine s
LEFT JOIN factor_oi f ON f.trade_date   = s.trade_date
LEFT JOIN oi_extra  o ON o.trade_date   = s.trade_date
LEFT JOIN vol_agg   v ON v.feature_date = s.trade_date
"""


def derivatives_sql(source: str = "levels") -> str:
    """d1 / d5 / 60-session z over the calendar spine. Same z construction as
    build_features: COUNT(x) >= 60 gate, STDDEV_SAMP, NULLIF denominator."""
    cols = []
    for m in CORE_A:
        if m == "total_oi":
            for n in (1, 5):
                cols.append(f"(total_oi - LAG(total_oi, {n}) OVER w)::DOUBLE"
                            f" / NULLIF(LAG(total_oi, {n}) OVER w, 0) AS d{n}_total_oi_pct_change")
        else:
            for n in (1, 5):
                cols.append(f"{m} - LAG({m}, {n}) OVER w AS d{n}_{m}_change")
        cols.append(f"CASE WHEN COUNT({m}) OVER w60 >= {Z_WINDOW}"
                    f" THEN ({m} - AVG({m}) OVER w60) / NULLIF(STDDEV_SAMP({m}) OVER w60, 0)"
                    f" ELSE NULL END AS zscore_{m}_3m")
    return (f"SELECT *,\n       " + ",\n       ".join(cols) + f"\nFROM {source}\n"
            f"WINDOW w AS (ORDER BY trade_date),\n"
            f"       w60 AS (ORDER BY trade_date ROWS BETWEEN {Z_WINDOW - 1} PRECEDING AND CURRENT ROW)")


# --- Reading -----------------------------------------------------------------

def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _year_files(store: Path, root: str, lo: date, hi: date) -> list[str]:
    out = []
    for p in sorted((store / root).glob("*.parquet")):
        if p.stem.isdigit() and lo.year <= int(p.stem) <= hi.year:
            out.append(p.as_posix())
    return out


def _scalar(sql: str):
    con = duckdb.connect(database=":memory:")
    try:
        return con.execute(sql).fetchone()[0]
    finally:
        con.close()


def store_first_date(store: Path, root: str) -> date | None:
    files = sorted(p for p in (store / root).glob("*.parquet") if p.stem.isdigit())
    if not files:
        return None
    return _scalar(f"SELECT MIN(trade_date) FROM read_parquet({_sql_str(files[0].as_posix())})")


def store_last_date(store: Path, root: str) -> date | None:
    files = sorted(p for p in (store / root).glob("*.parquet") if p.stem.isdigit())
    if not files:
        return None
    return _scalar(f"SELECT MAX(trade_date) FROM read_parquet({_sql_str(files[-1].as_posix())})")


def _union_sql(store: Path, lo: date, hi: date, select: str) -> str:
    parts = []
    for root in ROOTS:
        files = _year_files(store, root, lo, hi)
        if not files:
            raise SystemExit(f"no parquet under {store / root} for {lo.year}-{hi.year}")
        flist = "[" + ", ".join(_sql_str(f) for f in files) + "]"
        parts.append(f"SELECT '{root}' AS root, {select} FROM read_parquet({flist}) "
                     f"WHERE trade_date BETWEEN DATE '{lo}' AND DATE '{hi}'")
    return "\nUNION ALL\n".join(parts)


@lru_cache(maxsize=None)
def _next_td(d: date) -> date:
    return next_trading_day(d)


# --- Result ------------------------------------------------------------------

@dataclass
class TableA:
    frame: pd.DataFrame                # rows in [start, end_eff], COLUMNS_A + diagnostics
    full: pd.DataFrame                 # every spine row incl. lead-in
    start: date
    end_requested: date
    end_eff: date
    lead_in_wanted: int
    lead_in_used: int                  # lead-in sessions that carry OI
    vol_first: date                    # earliest volume session in the store (either root, latest of the two)
    split_events: dict = field(default_factory=dict)
    bad_split_factors: int = 0         # (root, trade_date) pairs whose factor is not 1.0
    notes: list = field(default_factory=list)


def compute_table_a(start: date, end: date, *, oi_store: Path, vol_store: Path,
                    splits: dict[str, pd.DataFrame], memory_limit: str = "3GB") -> TableA:
    """Compute table A for [start, end]. `splits` is {root: load_splits(...)}."""
    notes: list[str] = []

    # Cap at the last OI date both roots carry.
    oi_last = min(d for d in (store_last_date(oi_store, r) for r in ROOTS) if d is not None)
    end_eff = min(end, oi_last)
    if end_eff < end:
        notes.append(f"end capped at {end_eff}: last OI trade_date present in both roots")
    if end_eff < start:
        raise SystemExit(f"nothing to build: --start {start} is after the last OI date {oi_last}")

    out_sessions = get_trading_days(start, end_eff)
    if not out_sessions:
        raise SystemExit(f"no NYSE sessions in {start} .. {end_eff}")
    back = get_trading_days(start - timedelta(days=LEAD_IN_SESSIONS * 2 + 30), start - timedelta(days=1))
    lead = back[-LEAD_IN_SESSIONS:]
    spine = lead + out_sessions
    read_lo, read_hi = spine[0], spine[-1]

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET memory_limit = '{memory_limit}'")

    con.execute("CREATE TABLE spine (trade_date DATE)")
    con.executemany("INSERT INTO spine VALUES (?)", [(d,) for d in spine])

    # --- OI, split machinery kept in the read path (indices: factor 1.0) ----
    con.execute("CREATE TABLE oi_raw AS " + _union_sql(
        oi_store, read_lo, read_hi,
        "trade_date, expiration, strike, option_type, open_interest"))
    oi_dates = [r[0] for r in con.execute("SELECT DISTINCT trade_date FROM oi_raw ORDER BY 1").fetchall()]
    sf = []
    split_events = {}
    for root in ROOTS:
        sdf = splits.get(root, pd.DataFrame(columns=["trade_date", "splits"]))
        split_events[root] = len(sdf)
        f = make_split_factors(sdf, oi_dates)
        f["root"] = root
        sf.append(f)
    sf_df = pd.concat(sf, ignore_index=True)
    con.register("split_factors_df", sf_df)
    con.execute("CREATE TABLE split_factors AS SELECT root, CAST(trade_date AS DATE) AS trade_date, "
                "adj_factor::DOUBLE AS adj_factor FROM split_factors_df")
    con.execute("""
        CREATE VIEW oi AS
        SELECT raw.root, raw.trade_date, raw.expiration,
               raw.strike * COALESCE(sf.adj_factor, 1.0)        AS strike,
               raw.option_type,
               raw.open_interest / COALESCE(sf.adj_factor, 1.0) AS open_interest
        FROM oi_raw raw
        LEFT JOIN split_factors sf ON sf.root = raw.root AND sf.trade_date = raw.trade_date
    """)
    bad_sf = con.execute("SELECT COUNT(*) FROM split_factors WHERE adj_factor <> 1.0").fetchone()[0]

    # --- Factor-project OI aggregates, no spot ---------------------------------
    con.execute("CREATE TABLE ohlc (trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, "
                "close DOUBLE, volume DOUBLE)")
    con.execute("CREATE TABLE next_monthly_df (trade_date DATE, next_monthly DATE)")
    factor = con.execute(OI_FEATURES_SQL).df()
    missing = set(_FACTOR_OI_COLS) - set(factor.columns)
    if missing:
        raise SystemExit(f"build_features.OI_FEATURES_SQL no longer emits {sorted(missing)}")
    factor = factor[["trade_date", *_FACTOR_OI_COLS]].rename(columns=_FACTOR_OI_COLS)
    con.register("factor_df", factor)
    con.execute("CREATE TABLE factor_oi AS SELECT * REPLACE (CAST(trade_date AS DATE) AS trade_date) "
                "FROM factor_df")
    con.execute("CREATE TABLE oi_extra AS " + OI_EXTRA_SQL)

    # --- Volume: session column renamed on read, feature_date via the calendar --
    vol_lo = spine[0] - timedelta(days=10)
    vol_hi = spine[-1] - timedelta(days=1)
    con.execute("CREATE TABLE vol_raw AS " + _union_sql(
        vol_store, vol_lo, vol_hi,
        "trade_date AS vol_session, expiration, strike, option_type, volume"))
    sessions = [r[0] for r in con.execute("SELECT DISTINCT vol_session FROM vol_raw ORDER BY 1").fetchall()]
    fmap = pd.DataFrame({"vol_session": sessions,
                         "feature_date": [_next_td(s) for s in sessions]})
    con.register("fmap_df", fmap)
    con.execute("CREATE TABLE vol_feature_map AS SELECT CAST(vol_session AS DATE) AS vol_session, "
                "CAST(feature_date AS DATE) AS feature_date FROM fmap_df")
    con.execute("""
        CREATE VIEW vol AS
        SELECT r.root, r.vol_session, m.feature_date, r.expiration, r.strike, r.option_type, r.volume
        FROM vol_raw r JOIN vol_feature_map m USING (vol_session)
    """)
    # vol_feature_map is built from vol_raw's own sessions, so that JOIN cannot drop a row.
    n_raw, n_joined = con.execute(
        "SELECT (SELECT COUNT(*) FROM vol_raw), (SELECT COUNT(*) FROM vol)").fetchone()
    if n_raw != n_joined:
        raise SystemExit(f"volume rows lost mapping to feature_date: {n_raw} -> {n_joined}")
    con.execute("CREATE TABLE vol_agg AS " + VOL_AGG_SQL)

    con.execute("CREATE TABLE levels AS " + LEVELS_SQL)
    full = con.execute(derivatives_sql("levels") + "\nORDER BY trade_date").df()
    con.close()

    full = full[COLUMNS_A + DIAGNOSTICS_A].copy()
    for c in ("trade_date", "vol_session"):
        full[c] = pd.to_datetime(full[c]).dt.date
    frame = full[full["trade_date"] >= start].reset_index(drop=True)

    lead_rows = full[full["trade_date"] < start]
    lead_in_used = int(lead_rows["total_oi"].notna().sum())

    vol_first = max(d for d in (store_first_date(vol_store, r) for r in ROOTS) if d is not None)

    res = TableA(frame=frame, full=full, start=start, end_requested=end, end_eff=end_eff,
                 lead_in_wanted=LEAD_IN_SESSIONS, lead_in_used=lead_in_used,
                 vol_first=vol_first, split_events=split_events,
                 bad_split_factors=int(bad_sf), notes=notes)
    return res


# --- Checks --------------------------------------------------------------------

class Checker:
    def __init__(self) -> None:
        self.failed: list[str] = []

    def check(self, ok: bool, label: str) -> bool:
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        if not ok:
            self.failed.append(label)
        return ok

    def report(self, label: str) -> None:
        print(f"  [INFO] {label}")


def _show(df: pd.DataFrame, mask, cols, n: int = 5) -> str:
    bad = df.loc[mask, cols].head(n)
    return "" if bad.empty else "\n" + bad.to_string(index=False)


PCT_COLS = [c for c in CORE_A if c.startswith("pct_")]


def check_table_a(df: pd.DataFrame, *, sessions: list[date], vol_first: date,
                  chk: Checker, full: pd.DataFrame | None = None) -> None:
    """Hard checks on table A rows. `sessions` = the NYSE sessions the rows
    must cover exactly. `full` (rows incl. lead-in, contiguous on the spine)
    enables the z-score gating check."""
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Completeness
    have = list(df["trade_date"])
    chk.check(have == sessions and df["trade_date"].is_unique,
              f"exactly one row per NYSE session: {len(have)} rows, {len(sessions)} sessions")
    no_oi = df["total_oi"].isna() | (df.get("oi_roots", 2) != 2)
    chk.check(not no_oi.any(), f"every row carries OI from both roots ({int(no_oi.sum())} without)"
              + _show(df, no_oi, ["trade_date", "total_oi"]))

    # Previous NYSE session for every row, from the calendar.
    cal = get_trading_days(sessions[0] - timedelta(days=15), sessions[-1])
    prev = {cal[i]: cal[i - 1] for i in range(1, len(cal))}
    exp_prev = df["trade_date"].map(prev)
    vol_expected = exp_prev >= vol_first
    no_vol = vol_expected & (df["total_vol"].isna() | (df.get("vol_roots", 2) != 2))
    chk.check(not no_vol.any(),
              f"every row whose prior session is in the volume store carries volume from both roots "
              f"({int(no_vol.sum())} without)" + _show(df, no_vol, ["trade_date", "vol_session", "total_vol"]))
    pre = int((~vol_expected).sum())
    if pre:
        chk.report(f"{pre} row(s) precede volume coverage (prior session < {vol_first}); volume NULL there")

    # LOOKAHEAD — the hard check of spec §2.
    has_vol = df["total_vol"].notna()
    vs = df["vol_session"]
    one = df.get("n_vol_sessions", pd.Series(1, index=df.index))
    leak = has_vol & ~((one == 1) & (vs < df["trade_date"]) & (vs == exp_prev))
    chk.check(not leak.any(),
              f"LOOKAHEAD: every volume session contributing to a row is strictly earlier than its "
              f"trade_date and is the prior NYSE session — {int(has_vol.sum())} rows checked, "
              f"{int(leak.sum())} violations"
              + _show(df, leak, ["trade_date", "vol_session"]))

    # Sanity bounds
    for c in PCT_COLS:
        s = df[c].dropna()
        if c in ("pct_oi_spxw", "pct_vol_spxw"):
            ok = bool(((s > 0) & (s < 1)).all())
            chk.check(ok, f"{c} in (0, 1)  [min {s.min():.6g}, max {s.max():.6g}]" if len(s) else f"{c}: no values")
        else:
            ok = bool(((s >= 0) & (s <= 1)).all())
            chk.check(ok, f"{c} in [0, 1]  [min {s.min():.6g}, max {s.max():.6g}]" if len(s) else f"{c}: no values")
    s = df["weighted_avg_dte_oi"].dropna()
    chk.check(bool((s > 0).all()), f"weighted_avg_dte_oi > 0  [min {s.min():.4g}]")
    # REPORT, not a hard check: reliably true, but one thin-volume session with
    # a large LEAP block could flip it. List every such day; never fail the run.
    both = df["weighted_avg_dte_oi"].notna() & df["weighted_avg_dte_vol"].notna()
    flipped = both & ~(df["weighted_avg_dte_oi"] > df["weighted_avg_dte_vol"])
    chk.report(f"weighted_avg_dte_oi > weighted_avg_dte_vol: holds on {int((both & ~flipped).sum())} "
               f"of {int(both.sum())} rows; {int(flipped.sum())} day(s) where it does not"
               + ("" if not flipped.any() else "\n" + df.loc[
                   flipped, ["trade_date", "vol_session", "weighted_avg_dte_oi", "weighted_avg_dte_vol",
                             "total_vol"]].to_string(index=False)))
    for c in ("oi_put_call_ratio", "vol_put_call_ratio"):
        s = df[c].dropna()
        chk.check(bool((s > 0).all()), f"{c} > 0  [min {s.min():.4g}]" if len(s) else f"{c}: no values")

    # z gating: non-NULL only where the trailing 60 spine rows hold 60 non-NULL levels.
    if full is not None:
        f = full.sort_values("trade_date").reset_index(drop=True)
        in_out = f["trade_date"].isin(set(df["trade_date"]))
        bad = []
        for m in CORE_A:
            cnt = f[m].notna().astype(int).rolling(Z_WINDOW, min_periods=1).sum()
            z = f[f"zscore_{m}_3m"]
            if (in_out & z.notna() & (cnt < Z_WINDOW)).any():
                bad.append(m)
        chk.check(not bad, "z-scores NULL wherever the 60-session window holds < 60 observations"
                  + (f": violated by {bad}" if bad else ""))


def to_db_value(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if v is pd.NA or v is pd.NaT:
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    return v
