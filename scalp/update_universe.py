"""Nightly candidate list. One API call.

    python -m scalp.update_universe --date 2026-08-28
    python -m scalp.update_universe --dry-run

`snapshot/ohlc?symbol=*&venue=utp_cta`, run after the close, filtered to
price $100-$2,000 and dollar volume >= $100M. That yielded ~544 symbols on
2026-08-28 data. Thresholds are config values.

venue=utp_cta IS required here — this is the one endpoint where it was
measured to matter. The default `nqb` returns 44% of true volume and omits
~10,000 symbols, so the universe would be built from a Nasdaq-only view of
the market. The history endpoints are the opposite case and correctly send
nothing; see config.VENUE_BY_ENDPOINT.

HYSTERESIS: a name enters at >= $100 and >= $100M but is only dropped below
$85 or $70M. Boundary names would otherwise flicker in and out and leave
ragged history.

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
from datetime import date, timedelta

import pandas as pd

from scalp import config, db, schema, thetadata as td

log = logging.getLogger(__name__)


def classify(df: pd.DataFrame, prior: pd.DataFrame,
             trade_date: date) -> list[dict]:
    """Apply entry thresholds, hysteresis and stickiness. Returns universe rows.

    `prior` is the previous universe snapshot (may be empty on a first run).
    """
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
        })
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--date", default=None,
                    help="trade date to stamp (default: today ET)")
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

    log.info("universe for %s — one snapshot/ohlc call, venue=%s",
             trade_date, td.venue_for(td.EP_SNAPSHOT_OHLC))
    raw = td.snapshot_ohlc("*")
    df = raw.frame()
    log.info("  %d rows in %.1fs (%s)", len(df), raw.seconds,
             f"{raw.nbytes/1e6:.1f} MB")
    if df.empty:
        raise SystemExit("snapshot/ohlc returned nothing — is the session closed?")

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
        with db.connect() as conn:
            prior = pd.read_sql(
                "SELECT * FROM universe WHERE trade_date = %s", conn,
                params=(prior_date,))
        log.info("  prior universe %s: %d rows", prior_date, len(prior))

    rows = classify(norm, prior, trade_date)
    qualified = sum(1 for r in rows if r["qualified"])
    retained = sum(1 for r in rows if r["retained"])

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
    print(f"fetch list      : {qualified + retained:,}")

    if args.dry_run:
        print()
        print("--dry-run: nothing written.")
        return

    db.init_schema()
    n = db.write_universe(trade_date, rows)
    log.info("wrote %d universe rows for %s", n, trade_date)


if __name__ == "__main__":
    main()
