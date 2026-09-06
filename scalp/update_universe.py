"""Nightly candidate list. One API call.

    python -m scalp.update_universe --date 2026-08-28
    python -m scalp.update_universe --dry-run

`snapshot/ohlc?symbol=*&venue=utp_cta`, run after the close, filtered to
price $100-$2,000 and dollar volume >= $100M. That yielded ~544 symbols on
2026-08-28 data. Thresholds are config values.

--date DOES NOT FETCH HISTORICALLY, AND THE SCRIPT NOW REFUSES TO PRETEND IT
DOES. snapshot/ohlc is "now": asked for a past session it returns TODAY's
data, partial if the session is still open. Verified — `--date 2026-08-28`
run at 14:37 returned 335 qualifying names against 96 at 10:12, and both
would have been stamped 2026-08-28 as though the session had closed.

That is worse than having no row: the row looks finished, nothing downstream
can distinguish it from a real one, and every metric built on that universe
inherits the error invisibly. So a past --date is refused unless --from-eod is
passed, which reads `history/eod` — keyed by date, one request per symbol, and
correct.

venue=utp_cta IS required here — this is the one endpoint where it was
measured to matter. The default `nqb` returns 44% of true volume and omits
~10,000 symbols, so the universe would be built from a Nasdaq-only view of
the market. The history endpoints are the opposite case and correctly send
nothing; see config.VENUE_BY_ENDPOINT.

HYSTERESIS: a name enters at >= $50 and >= $100M but is only dropped below
$42.50 or $70M. Boundary names would otherwise flicker in and out and leave
ragged history. The exit price is a ratio of the entry price, so the band
follows the floor rather than having to be moved with it.

THE SPREAD FLOOR is a veto applied AFTER hysteresis and stickiness, not a term
inside the entry test. Folded in, it would exclude nothing on the night it was
applied: an incumbent that fails the floor still clears the looser exit
thresholds and still has a live 30-day sticky window, so `retained` would carry
it for another month.

It reads the last stored spread_bps_tw, which is a prediction -- a good one,
since a 2 bps name stays a 2 bps name, but a prediction. So it is reversible by
construction: every excluded name is re-measured on a cycle regardless of its
last score, and a name with no measurement at all is always included. Nothing
can be locked out by the absence of the evidence that would free it.

STICKINESS: once a symbol enters it stays in the fetch list for 30 days even
after it stops qualifying. Costs little, preserves continuity.

DOLLAR VOLUME IS A FLOOR, NOT A RANKING INPUT. Across the 15 traded tickers it
has no relationship to outcome — MRNA is highest at $3.6B and lost money; FDX
is near the bottom at $317M and was third best. It appears nowhere in the
ranking.
"""
from __future__ import annotations

import argparse
import logging
import time
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta

import pandas as pd

from scalp import config, db, schema, thetadata as td

log = logging.getLogger(__name__)


def spread_gate(symbol: str, spread: dict, trade_date: date) -> tuple:
    """(spread_bps, excluded, reason) for one symbol.

    Reasons, and why each exists:
      unmeasured  no stored spread at all. INCLUDED -- a new entrant gets one
                  session before being judged, and this is also the state every
                  symbol is in the first time the floor runs.
      retest      due for re-measurement. INCLUDED regardless of its last
                  score; this is the property that stops exclusion becoming
                  permanent, so it is checked BEFORE the threshold, not after.
      tight       measured below the floor and not due. EXCLUDED.
      wide        measured at or above the floor. INCLUDED.

    The re-test is staggered by a deterministic per-symbol phase rather than
    fired on age alone. Without it the whole excluded cohort is measured on the
    same night, excluded on the same night, and returns together every Nth
    night -- a permanent pulse of ~200 extra symbols on one night in ten
    instead of ~20 every night. The age check is kept as a backstop so a
    symbol cannot be starved by a run of missed nights.
    """
    got = spread.get(symbol)
    if got is None:
        return None, False, "unmeasured"
    value, measured = got
    n = max(1, config.UNIVERSE_SPREAD_RETEST_DAYS)
    phase = zlib.crc32(symbol.encode()) % n
    age = (trade_date - measured).days
    if age >= n or trade_date.toordinal() % n == phase:
        return value, False, "retest"
    if value < config.UNIVERSE_MIN_SPREAD_BPS:
        return value, True, "tight"
    return value, False, "wide"


def spread_lookup() -> dict:
    """symbol -> (last stored spread_bps_tw, the date it was measured).

    Empty on a database that has never computed the metric, which is the right
    degenerate behaviour: every symbol reads as `unmeasured` and the floor
    admits everything until there is something to judge on.
    """
    try:
        df = db.latest_metric_by_symbol(config.UNIVERSE_SPREAD_METRIC)
    except Exception as exc:
        log.warning("could not read %s (%s) — the spread floor admits "
                    "everything this run", config.UNIVERSE_SPREAD_METRIC, exc)
        return {}
    out = {}
    for r in df.itertuples():
        d = r.trade_date
        out[str(r.symbol).upper()] = (
            float(r.value), d.date() if hasattr(d, "date") else d)
    return out


def _after_close() -> bool:
    """True once the regular session has ended in ET."""
    now = pd.Timestamp.now(tz=config.MARKET_TZ)
    return now.time() >= pd.Timestamp(config.RTH_END).time()


def _eod_candidates(args) -> list[str]:
    """Symbols to price for a historical rebuild.

    The snapshot endpoint takes symbol=* and prices the whole market in one
    call. history/eod does not, so a rebuild has to name its candidates, and
    which list is used decides what the rebuilt universe CAN contain.

    --candidates roster      the full vendor roster. Complete, and slow.
    --candidates universe    symbols already in the universe table. Fast, and
                             structurally unable to find a name that qualified
                             on the target date but has never qualified since.
    --candidates A,B,C       an explicit list.

    The roster is the default because the fast option cannot answer the
    question the rebuild is being run to answer.
    """
    choice = args.candidates
    if choice == "roster":
        raw = td.list_symbols()
        df = raw.frame()
        col = schema.find(df, ["symbol", "root", "ticker"], "symbol")
        out = sorted({str(s).upper() for s in df[col].dropna()})
        log.info("  roster: %d symbols", len(out))
        return out
    if choice == "universe":
        with db.connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM universe ORDER BY symbol")
            out = [r[0] for r in cur.fetchall()]
        log.info("  universe table: %d symbols", len(out))
        if not out:
            raise SystemExit(
                "--candidates universe, but the universe table is empty. "
                "Use --candidates roster.")
        return out
    return [s.strip().upper() for s in choice.split(",") if s.strip()]


def _fetch_eod_one(symbol: str, day: date) -> dict | None:
    """Close and volume for one symbol on one settled session."""
    try:
        raw = td.history_eod(symbol, day, day)
    except td.NoDataError:
        return None
    except Exception as exc:
        log.debug("  %s: %s", symbol, exc)
        return None
    df = raw.frame()
    if df.empty:
        return None
    try:
        close_col = schema.find(df, ["close", "c"], "close")
        vol_col = schema.find(df, ["volume", "v"], "volume")
    except schema.SchemaError:
        return None
    close = pd.to_numeric(df[close_col], errors="coerce").dropna()
    vol = pd.to_numeric(df[vol_col], errors="coerce").dropna()
    if close.empty or vol.empty:
        return None
    # One row per session was requested, but take the last defensively.
    return {"symbol": symbol.upper(),
            "close": float(close.iloc[-1]),
            "volume": float(vol.iloc[-1])}


def _universe_from_eod(trade_date: date, args) -> pd.DataFrame:
    """Rebuild a past session's prices from history/eod, one symbol at a time.

    Slow by construction — there is no wildcard on this endpoint. The point is
    that a missed night stops being a permanent gap: without it the tool can
    only ever move forward from whenever it happens to be run.
    """
    symbols = _eod_candidates(args)
    workers = min(args.eod_workers, td.max_connections())
    log.info("rebuilding %s from history/eod — %d symbols, %d connections",
             trade_date, len(symbols), workers)
    log.info("this is one request per symbol; expect it to take minutes")

    rows: list[dict] = []
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_eod_one, s, trade_date): s
                   for s in symbols}
        for i, fut in enumerate(as_completed(futures), 1):
            got = fut.result()
            if got:
                rows.append(got)
            if i % 500 == 0 or i == len(symbols):
                elapsed = time.monotonic() - t0
                log.info("  %d/%d priced, %d with data, %.0f/s",
                         i, len(symbols), len(rows), i / max(elapsed, 1e-9))

    if not rows:
        raise SystemExit(
            f"history/eod returned nothing for any symbol on {trade_date}. "
            f"Was it a trading day?")
    log.info("  %d symbols priced on %s", len(rows), trade_date)
    return pd.DataFrame(rows)


def classify(df: pd.DataFrame, prior: pd.DataFrame, trade_date: date,
             spread: dict | None = None) -> list[dict]:
    """Apply entry thresholds, hysteresis, stickiness and the spread floor.

    `prior` is the previous universe snapshot (may be empty on a first run).
    `spread` is symbol -> (spread_bps, measured_date); an empty or omitted
    mapping admits everything, which is what makes the floor inert until the
    metric exists.
    """
    spread = spread or {}
    prior_by_symbol = ({r["symbol"]: r for _, r in prior.iterrows()}
                       if not prior.empty else {})

    rows = []
    for _, r in df.iterrows():
        symbol = str(r["symbol"]).upper()
        close = float(r["close"]) if pd.notna(r["close"]) else None
        volume = int(r["volume"]) if pd.notna(r["volume"]) else None
        dollar_vol = (close * volume) if (close and volume) else None
        was = prior_by_symbol.get(symbol)

        qualified = bool(
            close is not None and dollar_vol is not None
            and config.UNIVERSE_MIN_PRICE <= close <= config.UNIVERSE_MAX_PRICE
            and dollar_vol >= config.UNIVERSE_MIN_DOLLAR_VOL
        )

        # Hysteresis: an incumbent survives on the looser exit thresholds.
        incumbent = bool(was is not None and (was["qualified"] or was["retained"]))
        above_exit = bool(
            close is not None and dollar_vol is not None
            and close >= config.UNIVERSE_EXIT_PRICE
            and dollar_vol >= config.UNIVERSE_EXIT_DOLLAR_VOL
            and close <= config.UNIVERSE_MAX_PRICE
        )

        first_entered = (was["first_entered"] if was is not None
                         and was.get("first_entered") is not None else None)
        sticky_until = (was["sticky_until"] if was is not None
                        and was.get("sticky_until") is not None else None)

        if qualified:
            if first_entered is None:
                first_entered = trade_date
            sticky_until = trade_date + timedelta(days=config.UNIVERSE_STICKY_DAYS)

        sticky_live = bool(sticky_until is not None and trade_date <= sticky_until)
        retained = bool(not qualified and (
            (incumbent and above_exit) or sticky_live))

        # The spread floor. A veto on the fetch list, deliberately NOT a term
        # in `qualified` -- see the module docstring. It therefore does not
        # touch first_entered or sticky_until either: those record the
        # price/volume story, and a name excluded for being tight has not
        # stopped being a large, liquid name.
        spread_bps, spread_excluded, spread_reason = spread_gate(
            symbol, spread, trade_date)

        if not (qualified or retained):
            # Only carry a row for names that are in, or were in and are now
            # out — the whole market every night would bloat the table for no
            # analytical gain.
            if was is None:
                continue

        rows.append({
            "symbol": symbol, "close": close, "volume": volume,
            "dollar_volume": dollar_vol,
            "qualified": qualified, "retained": retained,
            "first_entered": first_entered, "sticky_until": sticky_until,
            "spread_bps": spread_bps, "spread_excluded": spread_excluded,
            "spread_reason": spread_reason,
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=None,
                    help="trade date (default: today ET). A PAST date needs "
                         "--from-eod: snapshot/ohlc is 'now' and cannot "
                         "retrieve a past session.")
    ap.add_argument("--from-eod", action="store_true",
                    help="rebuild from history/eod, which is keyed by date. "
                         "One request per symbol, so slow — but it is the only "
                         "way to reconstruct a session that has already closed.")
    ap.add_argument("--candidates", default="roster",
                    help="which symbols to price for --from-eod: 'roster' "
                         "(the full vendor list, default), 'universe' (names "
                         "already in the universe table — fast but cannot find "
                         "one that has since stopped qualifying), or an "
                         "explicit comma-separated list.")
    ap.add_argument("--eod-workers", type=int, default=4,
                    help="concurrent history/eod requests, capped at the "
                         "connection limit")
    ap.add_argument("--dry-run", action="store_true",
                    help="report the counts, write nothing")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s  %(levelname)-7s %(message)s",
                        datefmt="%H:%M:%S")

    trade_date = (date.fromisoformat(args.date) if args.date
                  else td.parse_date(pd.Timestamp.now(tz=config.MARKET_TZ)
                                     .strftime("%Y%m%d")))

    today = td.today_et()
    if trade_date < today and not args.from_eod:
        raise SystemExit(
            f"\nREFUSING: --date {trade_date} is in the past, and "
            f"snapshot/ohlc is \"now\".\n\n"
            f"That endpoint cannot retrieve a past session. Asked for one it "
            f"returns\nTODAY's data — partial, if the session is still open — "
            f"and this script would\nfile it under {trade_date} as though it "
            f"were a completed session.\n\n"
            f"That is worse than having no row at all: the row looks "
            f"finished, nothing\ndownstream can tell it is wrong, and every "
            f"metric built on that universe\ninherits the error silently.\n\n"
            f"To rebuild {trade_date} from data that genuinely comes from "
            f"{trade_date}:\n"
            f"    python -m scalp.update_universe --date {trade_date} "
            f"--from-eod\n\n"
            f"That uses history/eod, which is keyed by date. It is a "
            f"per-symbol loop\nrather than one wildcard call, so it is slow — "
            f"but it is correct."
        )

    if args.from_eod:
        norm = _universe_from_eod(trade_date, args)
    else:
        log.info("universe for %s — one snapshot/ohlc call, venue=%s",
                 trade_date, td.venue_for(td.EP_SNAPSHOT_OHLC))
        if trade_date == today and not _after_close():
            log.warning("the session is still open — this snapshot is "
                        "PARTIAL-DAY data. Run after the close, or the "
                        "dollar-volume filter reads low and the universe "
                        "comes out short.")
        raw = td.snapshot_ohlc("*")
        df = raw.frame()
        log.info("  %d rows in %.1fs (%s)", len(df), raw.seconds,
                 f"{raw.nbytes/1e6:.1f} MB")
        if df.empty:
            raise SystemExit(
                "snapshot/ohlc returned nothing — is the session closed?")

        sym_col = schema.find(df, ["symbol", "root", "ticker"], "symbol")
        close_col = schema.find(df, ["close", "c"], "close")
        vol_col = schema.find(df, ["volume", "v"], "volume")
        norm = pd.DataFrame({
            "symbol": df[sym_col],
            "close": pd.to_numeric(df[close_col], errors="coerce"),
            "volume": pd.to_numeric(df[vol_col], errors="coerce"),
        })

    prior_date = db.latest_universe_date()
    prior = pd.DataFrame()
    if prior_date is not None:
        prior = db.universe_on(prior_date)
        log.info("  prior universe %s: %d rows", prior_date, len(prior))

    spread = spread_lookup()
    log.info("  spread floor: %d symbol(s) with a stored %s",
             len(spread), config.UNIVERSE_SPREAD_METRIC)

    rows = classify(norm, prior, trade_date, spread)
    qualified = sum(1 for r in rows if r["qualified"])
    retained = sum(1 for r in rows if r["retained"])

    # The fetch list is what universe_symbols() will return, so it is counted
    # the same way here: in on price/volume, then minus the spread veto.
    in_universe = [r for r in rows if r["qualified"] or r["retained"]]
    excluded = [r for r in in_universe if r["spread_excluded"]]
    fetch_list = [r for r in in_universe if not r["spread_excluded"]]
    by_reason = {}
    for r in in_universe:
        by_reason[r["spread_reason"]] = by_reason.get(r["spread_reason"], 0) + 1

    print()
    print(f"trade_date      : {trade_date}")
    print(f"market rows     : {len(norm):,}")
    print(f"qualified       : {qualified:,}   "
          f"(price {config.UNIVERSE_MIN_PRICE:g}-{config.UNIVERSE_MAX_PRICE:g}, "
          f"$vol >= {config.UNIVERSE_MIN_DOLLAR_VOL/1e6:.0f}M)")
    print(f"retained        : {retained:,}   "
          f"(hysteresis below {config.UNIVERSE_EXIT_PRICE:g} / "
          f"{config.UNIVERSE_EXIT_DOLLAR_VOL/1e6:.0f}M, "
          f"or sticky {config.UNIVERSE_STICKY_DAYS}d)")
    print(f"in on price/vol : {len(in_universe):,}")
    print()
    print(f"spread floor    : >= {config.UNIVERSE_MIN_SPREAD_BPS:g} bps "
          f"(last stored {config.UNIVERSE_SPREAD_METRIC}, "
          f"re-tested every {config.UNIVERSE_SPREAD_RETEST_DAYS}d)")
    for reason, label in (
            ("wide",       "clears the floor"),
            ("tight",      "below the floor  -> EXCLUDED"),
            ("retest",     "due for re-test  -> kept"),
            ("unmeasured", "never measured   -> kept")):
        print(f"  {label:32s}: {by_reason.get(reason, 0):,}")
    print()
    print(f"FETCH LIST      : {len(fetch_list):,}   "
          f"({len(excluded):,} excluded by the spread floor)")

    if excluded:
        tightest = sorted(excluded, key=lambda r: r["spread_bps"])[:10]
        print(f"  tightest excluded: "
              + ", ".join(f"{r['symbol']} {r['spread_bps']:.1f}"
                          for r in tightest))

    if args.dry_run:
        print()
        print("--dry-run: nothing written.")
        return

    db.init_schema()
    n = db.write_universe(trade_date, rows)
    log.info("wrote %d universe rows for %s", n, trade_date)


if __name__ == "__main__":
    main()
