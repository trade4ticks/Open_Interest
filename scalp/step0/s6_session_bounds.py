"""Step 0.6 — where are the missing 19% of shares?

THE PROBLEM. On FDX 2026-08-28:

    trade_quote (default window)      776,192 shares
    snapshot/ohlc with utp_cta      ~ 774,000 shares
    EOD consolidated                  956,900 shares

Two independent endpoints agree with each other and both fall ~19% short. That
rules out tape coverage — the venue question is settled (s1: byte-identical
responses, 20 distinct exchange codes) and this is not it. Agreement between
two endpoints points at INCLUSION RULES, not at which tape is being read.

THE HYPOTHESIS. The closing cross falls outside the endpoint's default query
window. For a large cap the closing auction is routinely 5-15% of daily
volume, which is the right order of magnitude. Supporting evidence already in
hand: the first row of the default pull is a 20,056-share print at 09:30:01
with condition 62 (OPEN_REPORT), so the OPENING auction is inside the window.
If the default window is 09:30-16:00, the opening cross lands just inside it
and the closing cross lands just outside.

HOW THIS TESTS IT. Three pulls of the same symbol-day:

    A  no start_time/end_time      — whatever the default window is
    B  04:00:00 - 20:00:00         — everything the endpoint will give
    C  09:30:00 - 16:00:00         — explicit regular hours

A vs C settles what the default bounds actually are: byte-identical means the
default IS regular hours. B vs the EOD figure settles whether widening
recovers the gap.

Then it localises the recovered shares — before A's first print, inside A's
range, after A's last print — and reports the condition codes carrying them.
Condition 98 is `CLOSING`, documented by the vendor as market-centre closing
prints usable to identify the closing auction. If the recovered shares are
predominantly condition 98 sitting after A's last timestamp, the hypothesis is
confirmed directly rather than inferred from a volume delta.

    python -m scalp.step0.s6_session_bounds
    python -m scalp.step0.s6_session_bounds --symbol LLY --date 2026-08-28

WHAT THE OUTCOMES MEAN
  * B reaches 956,900, recovered shares are condition 98 after A's last print
        -> confirmed. fetch.py pulls 04:00-20:00 and filters to RTH afterward,
           rather than relying on the endpoint's default bounds. The closing
           cross is then available as its own feature instead of being lost.
  * B reaches 956,900 but the recovery is spread across the whole session
        -> NOT the closing cross. The default window is not the mechanism and
           something else is being filtered; do not close this out.
  * B still falls short of 956,900
        -> the gap is not session bounds at all. Next suspects are condition
           exclusions (odd lots, code 115) or the EOD figure counting prints
           this endpoint does not carry. Do not reconcile by assuming the
           remainder is noise — 19% is not noise.
  * A and C byte-identical AND B identical to both
        -> the endpoint ignores time bounds entirely, which contradicts s0.
           Stop and re-examine, because the Phase 2 intraday plan depends on
           those bounds working.
"""
from __future__ import annotations

import argparse

import pandas as pd

from scalp import config, thetadata as td
from scalp.step0 import _common as c


PULLS = [
    ("A  default window",      None,       None),
    ("B  04:00:00-20:00:00",   "04:00:00", "20:00:00"),
    ("C  09:30:00-16:00:00",   "09:30:00", "16:00:00"),
]


def _pull(symbol: str, day: str, label: str,
          start_time: str | None, end_time: str | None):
    print()
    print(f"[{label}]")
    try:
        raw = td.trade_quote(symbol, day, day,
                             start_time=start_time, end_time=end_time,
                             total_timeout=900)
    except Exception as exc:
        c.report_error(exc, label)
        return None, None
    df = raw.frame()
    print(f"  {len(df):,} rows  |  {raw.seconds:.2f}s  |  "
          f"{c.fmt_bytes(raw.nbytes)}")
    return raw, df


def _times(df: pd.DataFrame, col: str) -> pd.Series | None:
    """Trade timestamps as datetimes, or None if the column can't be read."""
    s = df[col]
    if pd.api.types.is_numeric_dtype(s):
        lo, hi = s.min(), s.max()
        if 0 <= lo and hi <= 86_400_000:          # ms since ET midnight
            return pd.to_datetime(s, unit="ms")
        return None
    parsed = pd.to_datetime(s, errors="coerce")
    return None if parsed.isna().all() else parsed


def _sizes(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df[col], errors="coerce").fillna(0)


def _cond_label(row: pd.Series, cond_cols: list[str]) -> str:
    """All condition codes on one row, labelled where the table knows them."""
    parts = []
    for col in cond_cols:
        val = row[col]
        if pd.isna(val):
            continue
        try:
            code = int(val)
        except (TypeError, ValueError):
            continue
        if code in config.NO_CONDITION_SENTINELS:
            continue          # 0 and 255 are padding on ext_condition*, not codes
        parts.append(f"{col.replace('ext_condition', 'ext')}={code} "
                     f"({config.condition_name(code)})")
    return "; ".join(parts) if parts else "(none)"


def _row_codes(df: pd.DataFrame, cond_cols: list[str]) -> pd.Series:
    """Per row, the set of real condition codes across every condition column."""
    def codes(row) -> set[int]:
        out = set()
        for col in cond_cols:
            val = row[col]
            if pd.isna(val):
                continue
            try:
                code = int(val)
            except (TypeError, ValueError):
                continue
            if code not in config.NO_CONDITION_SENTINELS:
                out.add(code)
        return out
    if not cond_cols:
        return pd.Series([set()] * len(df), index=df.index)
    return df.apply(codes, axis=1)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--symbol", default=config.VENUE_CHECK_SYMBOL)
    ap.add_argument("--date", default=config.VENUE_CHECK_DATE)
    ap.add_argument("--expected", type=int, default=config.VENUE_CHECK_EXPECTED)
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    c.setup_logging(args.verbose)
    c.banner(f"STEP 0.6 — SESSION BOUNDS AND THE MISSING VOLUME "
             f"({args.symbol} {args.date})")
    c.env_summary()
    print()
    print(f"EOD consolidated reference : {args.expected:,} shares")

    results: dict[str, tuple] = {}
    for label, st, et in PULLS:
        raw, df = _pull(args.symbol, args.date, label, st, et)
        if raw is not None and df is not None and not df.empty:
            results[label] = (raw, df)

    if not results:
        c.die("Every pull failed or came back empty — nothing to diagnose.")

    # --- what ARE the default bounds? ---------------------------------------
    c.section("what the default window actually is")
    labels = list(results)
    for i, a in enumerate(labels):
        for b in labels[i + 1:]:
            same = results[a][0].body == results[b][0].body
            print(f"  {a}  vs  {b}  ->  "
                  f"{'BYTE-IDENTICAL' if same else 'different'}")
    print()
    print("A identical to C means the default window IS regular hours, and the")
    print("closing cross is outside it by construction.")

    # --- resolve columns ----------------------------------------------------
    first_df = next(iter(results.values()))[1]
    c.section("column resolution")
    tcol = c.find_column(first_df, c.CAND_TRADE_TIME, "trade timestamp")
    scol = c.find_column(first_df, c.CAND_TRADE_SIZE, "trade size")
    pcol = c.find_column(first_df, c.CAND_TRADE_PRICE, "trade price",
                         required=False)
    xcol = c.find_column(first_df, c.CAND_EXCHANGE, "exchange", required=False)
    cond_cols = c.condition_columns(first_df)
    print(f"  {'condition columns':<22s} -> {cond_cols or 'NONE'}")

    # --- totals and session extent ------------------------------------------
    c.section("totals and session extent")
    print(f"{'pull':<24s} {'rows':>9s} {'shares':>13s} {'vs EOD':>9s}  "
          f"{'first print':>12s} {'last print':>12s}")
    print("-" * 88)
    extents: dict[str, tuple] = {}
    for label, (raw, df) in results.items():
        total = int(_sizes(df, scol).sum())
        ts = _times(df, tcol)
        if ts is not None:
            first, last = ts.min(), ts.max()
            extents[label] = (first, last, ts)
            first_s, last_s = first.strftime("%H:%M:%S"), last.strftime("%H:%M:%S")
        else:
            first_s = last_s = "unreadable"
        pct = 100 * total / args.expected if args.expected else float("nan")
        print(f"{label:<24s} {len(df):>9,} {total:>13,} {pct:>8.1f}%  "
              f"{first_s:>12s} {last_s:>12s}")

    print()
    print("The LAST PRINT of the default pull is the single most informative")
    print("number here. 15:59:59 with a wider pull running to 16:00:0x means")
    print("the closing cross is being cut off.")

    # --- volume by hour -----------------------------------------------------
    for label, (raw, df) in results.items():
        ts = _times(df, tcol)
        if ts is None:
            continue
        c.section(f"volume by hour — {label}")
        by_hour = pd.DataFrame({
            "hour": ts.dt.strftime("%H:00"),
            "shares": _sizes(df, scol),
        }).groupby("hour").agg(trades=("shares", "size"),
                               shares=("shares", "sum"))
        by_hour["pct_of_day"] = 100 * by_hour["shares"] / by_hour["shares"].sum()
        by_hour["pct_of_eod"] = 100 * by_hour["shares"] / args.expected
        with pd.option_context("display.float_format", "{:,.2f}".format):
            print(by_hour.to_string())

    # --- where the recovered shares sit -------------------------------------
    a_label = "A  default window"
    b_label = "B  04:00:00-20:00:00"
    reconciled: int | None = None
    if a_label in extents and b_label in results:
        a_first, a_last, _ = extents[a_label]
        _, b_df = results[b_label]
        b_ts = _times(b_df, tcol)
        if b_ts is not None:
            c.section("where the shares outside the default window sit")
            b_sizes = _sizes(b_df, scol)
            before = b_ts < a_first
            after = b_ts > a_last
            inside = ~before & ~after
            total_b = int(b_sizes.sum())
            for name, mask in (("before default first print", before),
                               ("inside default range", inside),
                               ("after default last print", after)):
                sh = int(b_sizes[mask].sum())
                print(f"  {name:<30s} {int(mask.sum()):>8,} trades  "
                      f"{sh:>12,} shares  ({100 * sh / max(total_b, 1):5.1f}% of B)")

            # Condition composition of the shares outside the default window.
            outside = before | after
            if cond_cols and outside.any():
                print()
                print("  condition codes on shares OUTSIDE the default window:")
                rows = []
                for col in cond_cols:
                    sub = b_df.loc[outside, [col]].copy()
                    sub["shares"] = b_sizes[outside].values
                    for code, grp in sub.groupby(col, dropna=True):
                        try:
                            code_i = int(code)
                        except (TypeError, ValueError):
                            continue
                        if code_i in config.NO_CONDITION_SENTINELS:
                            continue
                        rows.append({
                            "column": col,
                            "code": code_i,
                            "name": config.condition_name(code_i),
                            "trades": len(grp),
                            "shares": int(grp["shares"].sum()),
                            "auction": code_i in config.AUCTION_PRINT_CONDITION_CODES,
                            "restate": code_i in config.RESTATEMENT_CONDITION_CODES,
                            "ext_hours": code_i in config.EXTENDED_HOURS_CONDITION_CODES,
                        })
                if rows:
                    tbl = (pd.DataFrame(rows)
                           .sort_values("shares", ascending=False)
                           .head(25))
                    print(tbl.to_string(index=False))
                    print()
                    print("  Read the `auction` and `restate` columns, not just the")
                    print("  share totals. Condition 98/62 are genuine auction")
                    print("  executions; 51/66 RE-REPORT the same shares hours")
                    print("  later and must never be added to them.")

            # --- reconciliation, counting each auction once -----------------
            # A raw sum over the wide window double-counts: the official close
            # is restated on a schedule for hours afterwards. The honest
            # reconciliation is the RTH total plus the genuine auction prints
            # that fall outside it, each counted once.
            if cond_cols:
                b_codes = _row_codes(b_df, cond_cols)
                is_restate = b_codes.apply(
                    lambda s: bool(s & config.RESTATEMENT_CONDITION_CODES))
                is_auction = b_codes.apply(
                    lambda s: bool(s & config.AUCTION_PRINT_CONDITION_CODES))
                a_total = int(_sizes(results[a_label][1], scol).sum())
                outside_auction = outside & is_auction & ~is_restate
                auction_shares = int(b_sizes[outside_auction].sum())
                restated_shares = int(b_sizes[outside & is_restate].sum())
                recon = a_total + auction_shares
                reconciled = recon

                c.section("reconciliation, each auction counted once")
                print(f"  RTH prints (default window)      {a_total:>12,}")
                print(f"  auction prints outside RTH       {auction_shares:>12,}  "
                      f"({int(outside_auction.sum())} print(s))")
                print(f"                                   {'-' * 12}")
                print(f"  reconciled total                 {recon:>12,}")
                print(f"  EOD consolidated reference       {args.expected:>12,}")
                delta = recon - args.expected
                print(f"  difference                       {delta:>+12,}  "
                      f"({100 * delta / args.expected:+.2f}%)")
                print()
                print(f"  restatement shares excluded      {restated_shares:>12,}  "
                      f"({int((outside & is_restate).sum())} print(s))")
                print("  Those are re-reports of shares already counted above.")
                print("  Adding them is what makes a wide-window sum overshoot.")

    # --- largest prints of the day ------------------------------------------
    target = results.get(b_label) or next(iter(results.values()))
    _, df = target
    c.section("ten largest prints of the day (widest pull available)")
    sizes = _sizes(df, scol)
    top = df.assign(_size=sizes).nlargest(10, "_size")
    ts = _times(df, tcol)
    for _, row in top.iterrows():
        when = "?"
        if ts is not None:
            when = ts.loc[row.name].strftime("%H:%M:%S.%f")[:-3]
        price = f"{row[pcol]:.2f}" if pcol else "?"
        exch = config.exchange_name(row[xcol]) if xcol else "?"
        pct = 100 * row["_size"] / args.expected if args.expected else float("nan")
        print(f"  {when}  {int(row['_size']):>9,} sh ({pct:4.1f}% of EOD)  "
              f"@ {price:>9s}  {exch}")
        print(f"      {_cond_label(row, cond_cols)}")

    # --- verdict ------------------------------------------------------------
    c.banner("WHAT TO CONCLUDE")
    if b_label in results:
        b_total = int(_sizes(results[b_label][1], scol).sum())
        b_pct = 100 * b_total / args.expected
        print(f"Raw sum over the widest window: {b_total:,} of "
              f"{args.expected:,} ({b_pct:.1f}% of EOD).")

        # An OVERSHOOT is not a smaller version of an undershoot — it is a
        # different finding with a different cause, and treating it as "short
        # by a negative number" is how this script read 180% as a shortfall.
        if b_pct > 105:
            print()
            print("OVERSHOOT — the wide window DOUBLE-COUNTS. The official open")
            print("and close are re-reported for hours after the session under")
            print("codes 51 and 66, carrying the full auction size each time.")
            print("A raw sum over a wide window is therefore meaningless; use")
            print("the reconciliation above, not this total.")

        if reconciled is not None:
            delta = reconciled - args.expected
            print()
            print(f"Reconciled (auctions counted once): {reconciled:,} vs "
                  f"{args.expected:,} EOD ({100 * delta / args.expected:+.2f}%).")
            if abs(delta) <= args.expected * 0.01:
                print()
                print("RESOLVED. There was no missing volume. The closing cross")
                print("prints seconds after 16:00 and so falls outside the")
                print("default window by construction — which is CORRECT for")
                print("this pipeline. Auction prints carry no meaningful quote")
                print("and would corrupt spread and noise if included.")
                print()
                print("Keep pulling RTH only. Trade counts and spreads were")
                print("never contaminated.")
            else:
                print()
                print("Reconciliation does not land within 1%. Something beyond")
                print("session bounds and restatements is in play — do not close")
                print("this out, and do not build trade-count metrics yet.")
        else:
            print()
            print("No condition columns, so auctions could not be separated from")
            print("restatements and no reconciliation was possible.")
    print()
    print("Set config.VOLUME_GAP_RESOLVED = True only when the remaining")
    print("difference is understood, not merely when it is small.")


if __name__ == "__main__":
    main()
