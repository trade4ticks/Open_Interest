"""Step 0.4 — which trade condition codes are present, and which to exclude?

Every metric built on the trade tape — trades per minute, trade size, the
at-bid/at-ask classification that measures two-sidedness — is wrong if it
counts prints that are not executions at the prevailing quote. Odd-lot prints,
derivatively-priced prints, out-of-sequence corrections and Form-T extended
hours prints all land in the same response as ordinary executions.

So enumerate what is actually there before deciding anything. For each code
this reports:

  * share of trades and share of volume
  * median distance from the prevailing midpoint, in bps
  * how often the print sits outside the NBBO entirely

That last pair is what makes the exclusion decision, not the code's name. A
code whose prints cluster at the mid is a normal execution; one whose prints
are routinely 30 bps off the quote is not something to count as an arrival.

Run across several symbols — codes vary by name and by venue, and one symbol's
tape is not the roster.

    python -m scalp.step0.s4_conditions
    python -m scalp.step0.s4_conditions --symbols FDX,LLY,LITE,DLTR

WHAT A BAD RESULT LOOKS LIKE
  * No condition column at all -> exclusions cannot be made on this endpoint.
    Say so; it means trades/min counts every print including odd lots, and the
    flow metrics carry a known bias that has to be documented rather than
    silently absorbed.
  * One code covering >90% of prints with the rest negligible -> fine, and the
    exclusion list is short.
  * A large share of prints far off the mid -> those are the ones to exclude,
    and the at-bid/at-ask classification must run AFTER the exclusion.
"""
from __future__ import annotations

import argparse

import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


def _analyse_symbol(symbol: str, day: str) -> pd.DataFrame | None:
    print()
    print(f"[{symbol} {day}]")
    try:
        raw = td.trade_quote(symbol, day, day, total_timeout=900)
    except Exception as exc:
        c.report_error(exc, symbol)
        return None

    df = raw.frame()
    print(f"  {len(df):,} rows in {raw.seconds:.1f}s")
    if df.empty:
        return None
    return df


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbols", default="FDX,LLY,LITE,DLTR",
                    help="comma-separated; spans the calibration range")
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    c.banner(f"STEP 0.4 — TRADE CONDITION CODES ({args.date})")
    c.env_summary()
    print()
    print(f"symbols       : {', '.join(symbols)}")

    frames: dict[str, pd.DataFrame] = {}
    for sym in symbols:
        df = _analyse_symbol(sym, args.date)
        if df is not None:
            frames[sym] = df

    if not frames:
        c.die("No data for any symbol — nothing to enumerate.")

    first = next(iter(frames.values()))
    cond_cols = c.condition_columns(first)

    c.section("condition columns found")
    if not cond_cols:
        print("NONE. Columns present:")
        print(f"  {list(first.columns)}")
        print()
        print("FINDING: trade_quote does not expose condition codes on this")
        print("subscription. Exclusions cannot be applied, so trades/min and")
        print("the at-bid/at-ask classification will count every print,")
        print("including odd lots and out-of-sequence corrections. That is a")
        print("documented bias, not a bug — but it has to be written down in")
        print("the metric definitions rather than discovered later.")
        return
    print(f"  {cond_cols}")

    # --- resolve the fields the off-quote analysis needs ---------------------
    c.section("column resolution")
    price = c.find_column(first, c.CAND_TRADE_PRICE, "trade price", required=False)
    size = c.find_column(first, c.CAND_TRADE_SIZE, "trade size", required=False)
    bid = c.find_column(first, c.CAND_BID, "bid", required=False)
    ask = c.find_column(first, c.CAND_ASK, "ask", required=False)
    exch = c.find_column(first, c.CAND_EXCHANGE, "exchange", required=False)

    for sym, df in frames.items():
        c.section(f"{sym} — condition code census")
        for col in cond_cols:
            counts = df[col].value_counts(dropna=False)
            print(f"\ncolumn {col!r}: {len(counts)} distinct value(s)")

            rows = []
            total_trades = len(df)
            total_vol = (pd.to_numeric(df[size], errors="coerce").fillna(0).sum()
                         if size else 0)

            for code, n in counts.head(40).items():
                sub = df[df[col] == code] if pd.notna(code) else df[df[col].isna()]
                vol = (pd.to_numeric(sub[size], errors="coerce").fillna(0).sum()
                       if size else 0)
                rec = {
                    "code": repr(code)[:20],
                    "name": config.condition_name(code),
                    "trades": n,
                    "trade_pct": 100 * n / total_trades,
                    "vol_pct": (100 * vol / total_vol) if total_vol else float("nan"),
                }
                if price and bid and ask:
                    p = pd.to_numeric(sub[price], errors="coerce")
                    b = pd.to_numeric(sub[bid], errors="coerce")
                    a = pd.to_numeric(sub[ask], errors="coerce")
                    mid = (a + b) / 2
                    ok = mid > 0
                    rec["off_mid_bps"] = float(
                        ((p[ok] - mid[ok]).abs() / mid[ok] * 10_000).median()
                    ) if ok.any() else float("nan")
                    outside = ((p < b) | (p > a)) & b.notna() & a.notna()
                    rec["outside_nbbo_pct"] = 100 * float(outside.mean())
                if size:
                    rec["median_size"] = float(
                        pd.to_numeric(sub[size], errors="coerce").median()
                    )
                rows.append(rec)

            table = pd.DataFrame(rows)
            with pd.option_context("display.max_columns", None,
                                   "display.width", 200,
                                   "display.float_format", "{:,.2f}".format):
                print(table.to_string(index=False))

        if exch:
            c.section(f"{sym} — exchange codes and off-exchange share")
            ex_counts = df[exch].value_counts()
            vol = (pd.to_numeric(df[size], errors="coerce").fillna(0)
                   if size else None)
            rows = []
            for code, n in ex_counts.items():
                mask = df[exch] == code
                rows.append({
                    "code": code,
                    "name": config.exchange_name(code),
                    "off_exch": config.is_off_exchange(code),
                    "trades": n,
                    "trade_pct": 100 * n / len(df),
                    "share_pct": (100 * vol[mask].sum() / vol.sum()
                                  if vol is not None and vol.sum() else float("nan")),
                })
            tbl = pd.DataFrame(rows).sort_values("trades", ascending=False)
            with pd.option_context("display.max_columns", None,
                                   "display.width", 200,
                                   "display.float_format", "{:,.2f}".format):
                print(tbl.head(30).to_string(index=False))

            # off_exchange_share, computed the way the metric layer will.
            off = df[exch].map(config.is_off_exchange)
            print()
            print(f"  off_exchange_share (trades) : {100 * off.mean():.2f}%")
            if vol is not None and vol.sum():
                print(f"  off_exchange_share (shares) : "
                      f"{100 * vol[off].sum() / vol.sum():.2f}%")
            unknown = [code for code in ex_counts.index
                       if config.exchange_name(code).startswith("unknown")]
            if unknown:
                print(f"  UNKNOWN exchange codes present: {unknown}")
                print("  These are not counted as off-exchange. A code missing")
                print("  from the table means the vendor added a venue — add it")
                print("  to config.EXCHANGE_NAMES rather than letting it sit in")
                print("  the on-exchange bucket by default.")

    c.banner("HOW TO DECIDE THE EXCLUSION LIST")
    print("Exclude a code when its prints are systematically off-quote —")
    print("high off_mid_bps or a high outside_nbbo_pct — not because its name")
    print("sounds irregular. A code that clusters at the mid is an ordinary")
    print("execution regardless of what it is called.")
    print()
    print("Codes appearing in one symbol but not another are normal. The union")
    print("across symbols is the roster; anything unseen here gets logged and")
    print("counted rather than silently dropped when it first appears.")


if __name__ == "__main__":
    main()
