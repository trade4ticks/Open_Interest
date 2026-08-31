"""Data-quality guards for the trade tape.

Right now this holds one guard: RESTATEMENT DETECTION.

WHY IT EXISTS. On FDX 2026-08-28 the closing auction printed 186,344 shares at
$330.88 on NYSE at 16:00:03.158 under condition 98 (CLOSING). The same size, at
the same price, on the same exchange, then printed four more times — 16:00:03,
16:10:00, 18:30:00 and 19:00:00 — under condition 51 (MC_OFFICIAL_CLOSE). Those
are re-reports of one execution, not five executions. Summing them made a
wide-window pull report 180% of the day's true volume. The opening auction does
the same thing under 62 then 66.

TWO LAYERS, IN ORDER OF RELIABILITY.

  1. Condition codes (config.RESTATEMENT_CONDITION_CODES). Primary. The vendor
     labels these prints and the labels are unambiguous.

  2. The (size, price, exchange) signature in this module. A BACKSTOP for a
     restatement arriving under a code we have not catalogued — the condition
     table is known-incomplete, and an unlabelled code carrying a duplicated
     block would otherwise pass straight through.

WHY THE SIZE THRESHOLD IS LOAD-BEARING. The signature "identical size, price
and exchange more than a second apart" describes an auction restatement, but it
also describes perfectly ordinary trading: a 100-share print at the same price
on the same venue recurs dozens of times a day in any liquid name, entirely
legitimately. Without a floor this guard would flag a large fraction of a normal
tape. RESTATEMENT_MIN_SHARES keeps it pointed at the class of event it was built
for.

IT FLAGS, IT DOES NOT DROP. Every function here returns markers. The decision to
exclude rows belongs to the metric layer, where it is visible, and the condition
codes do the real work — this is the net underneath them.

    python -m scalp.quality --selftest
"""
from __future__ import annotations

import pandas as pd

from scalp import config


def condition_codes_per_row(df: pd.DataFrame,
                            condition_cols: list[str]) -> pd.Series:
    """Per row, the set of real condition codes across every condition column.

    A print carries its condition in `condition` and may repeat or extend it
    across `ext_condition1..4`; the restatement marker showed up in both on
    FDX. Checking only the primary column would have missed it. 0 and 255 are
    padding, not codes.
    """
    if not condition_cols:
        return pd.Series([set()] * len(df), index=df.index, dtype=object)

    def codes(row) -> set[int]:
        out: set[int] = set()
        for col in condition_cols:
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

    return df[condition_cols].apply(codes, axis=1)


def flag_restatement_codes(df: pd.DataFrame,
                           condition_cols: list[str]) -> pd.Series:
    """True where any condition column marks the row as a restatement."""
    per_row = condition_codes_per_row(df, condition_cols)
    return per_row.apply(lambda s: bool(s & config.RESTATEMENT_CONDITION_CODES))


def find_duplicate_prints(
    df: pd.DataFrame,
    *,
    size_col: str,
    price_col: str,
    exchange_col: str,
    time_col: str,
    min_shares: int = config.RESTATEMENT_MIN_SHARES,
    min_seconds: float = config.RESTATEMENT_MIN_SECONDS,
) -> pd.DataFrame:
    """Flag repeated large prints sharing (size, price, exchange).

    Returns a frame indexed like `df` with:

        dup_group     group id, or <NA> when the row is not in a flagged group
        dup_first     True for the earliest print in a flagged group (the one
                      that is presumed genuine and should be KEPT)
        dup_repeat    True for every later print in a flagged group (the ones
                      that look like re-reports)

    A group is flagged only when it holds more than one print, the print is at
    least `min_shares`, and the span between first and last exceeds
    `min_seconds`. Two fills of the same size and price within the same second
    are ordinary market activity, not a restatement.
    """
    out = pd.DataFrame(index=df.index)
    out["dup_group"] = pd.Series(pd.NA, index=df.index, dtype="Int64")
    out["dup_first"] = False
    out["dup_repeat"] = False

    if df.empty:
        return out

    sizes = pd.to_numeric(df[size_col], errors="coerce")
    times = pd.to_datetime(df[time_col], errors="coerce")
    candidates = df.index[(sizes >= min_shares) & times.notna()]
    if len(candidates) < 2:
        return out

    work = pd.DataFrame({
        "size": sizes.loc[candidates],
        "price": pd.to_numeric(df[price_col], errors="coerce").loc[candidates],
        "exchange": df[exchange_col].loc[candidates],
        "time": times.loc[candidates],
    })

    group_id = 0
    for _, grp in work.groupby(["size", "price", "exchange"], dropna=True):
        if len(grp) < 2:
            continue
        span = (grp["time"].max() - grp["time"].min()).total_seconds()
        if span <= min_seconds:
            continue           # same instant — ordinary activity, not a re-report
        ordered = grp.sort_values("time")
        out.loc[ordered.index, "dup_group"] = group_id
        out.loc[ordered.index[0], "dup_first"] = True
        out.loc[ordered.index[1:], "dup_repeat"] = True
        group_id += 1

    return out


def duplicate_print_report(df: pd.DataFrame, dup: pd.DataFrame, *,
                           size_col: str, price_col: str,
                           exchange_col: str, time_col: str) -> pd.DataFrame:
    """One row per flagged group, worst first, for eyeballing."""
    rows = []
    for gid, idx in dup.dropna(subset=["dup_group"]).groupby("dup_group").groups.items():
        grp = df.loc[idx]
        times = pd.to_datetime(grp[time_col], errors="coerce").sort_values()
        size = pd.to_numeric(grp[size_col], errors="coerce").iloc[0]
        rows.append({
            "group": int(gid),
            "prints": len(grp),
            "size": int(size),
            "price": grp[price_col].iloc[0],
            "exchange": config.exchange_name(grp[exchange_col].iloc[0]),
            "first": times.iloc[0].strftime("%H:%M:%S.%f")[:-3],
            "last": times.iloc[-1].strftime("%H:%M:%S.%f")[:-3],
            "excess_shares": int(size * (len(grp) - 1)),
        })
    if not rows:
        return pd.DataFrame(columns=["group", "prints", "size", "price",
                                     "exchange", "first", "last",
                                     "excess_shares"])
    return (pd.DataFrame(rows)
            .sort_values("excess_shares", ascending=False)
            .reset_index(drop=True))


def audit_symbol_day(
    df: pd.DataFrame,
    *,
    size_col: str,
    price_col: str,
    exchange_col: str,
    time_col: str,
    condition_cols: list[str],
) -> dict:
    """Both layers at once, for one symbol-day. Returns a summary dict.

    `code_flagged_not_dup` and `dup_not_code_flagged` are the interesting
    numbers. The second is what this guard exists to surface: a duplicated
    block that the condition table did not label.
    """
    by_code = flag_restatement_codes(df, condition_cols)
    dup = find_duplicate_prints(df, size_col=size_col, price_col=price_col,
                                exchange_col=exchange_col, time_col=time_col)
    sizes = pd.to_numeric(df[size_col], errors="coerce").fillna(0)

    return {
        "rows": len(df),
        "shares_raw": int(sizes.sum()),
        "code_restatements": int(by_code.sum()),
        "code_restatement_shares": int(sizes[by_code].sum()),
        "dup_repeats": int(dup["dup_repeat"].sum()),
        "dup_repeat_shares": int(sizes[dup["dup_repeat"]].sum()),
        "code_flagged_not_dup": int((by_code & ~dup["dup_repeat"]).sum()),
        "dup_not_code_flagged": int((dup["dup_repeat"] & ~by_code).sum()),
        "report": duplicate_print_report(df, dup, size_col=size_col,
                                         price_col=price_col,
                                         exchange_col=exchange_col,
                                         time_col=time_col),
    }


# --- self test ---------------------------------------------------------------

def _selftest() -> None:
    """Synthetic tape reproducing both the real case and the false-positive risk.

    No network, no live data. Run it to confirm the guard catches the FDX
    restatement pattern without flagging ordinary round-lot repetition.
    """
    day = "2026-08-28T"
    rows = [
        # The real pattern: one closing auction, re-reported four times.
        (f"{day}16:00:03.158", 186_344, 330.88, 3, 98, 255),
        (f"{day}16:00:03.212", 186_344, 330.88, 3, 51, 255),
        (f"{day}16:10:00.001", 186_344, 330.88, 3, 51, 255),
        (f"{day}18:30:00.001", 186_344, 330.88, 3, 51, 255),
        (f"{day}19:00:00.002", 186_344, 330.88, 3, 51, 255),
        # The opening auction: genuine print plus one restatement.
        (f"{day}09:30:01.000",  20_056, 332.38, 3, 62, 255),
        (f"{day}09:30:01.000",  20_056, 332.38, 3, 66, 255),
        # Ordinary round lots at one price on one venue, spread over the day.
        # These MUST NOT be flagged — this is the false-positive case.
        (f"{day}10:15:00.100",     100, 331.00, 57, 0, 255),
        (f"{day}11:20:31.400",     100, 331.00, 57, 0, 255),
        (f"{day}13:44:02.900",     100, 331.00, 57, 0, 255),
        (f"{day}14:51:19.050",     100, 331.00, 57, 0, 255),
        # A large duplicated block under NO restatement code — exactly what
        # the backstop exists for.
        (f"{day}11:00:00.000",  50_000, 329.50, 60, 0, 255),
        (f"{day}11:45:00.000",  50_000, 329.50, 60, 0, 255),
    ]
    df = pd.DataFrame(rows, columns=["trade_timestamp", "size", "price",
                                     "exchange", "condition", "ext_condition1"])

    result = audit_symbol_day(
        df, size_col="size", price_col="price", exchange_col="exchange",
        time_col="trade_timestamp",
        condition_cols=["condition", "ext_condition1"],
    )

    print("synthetic tape:", result["rows"], "rows,",
          f"{result['shares_raw']:,} shares raw")
    print()
    print(f"restatements by condition code : {result['code_restatements']} rows, "
          f"{result['code_restatement_shares']:,} shares")
    print(f"repeats by (size,price,exch)   : {result['dup_repeats']} rows, "
          f"{result['dup_repeat_shares']:,} shares")
    print(f"code-flagged but not duplicate : {result['code_flagged_not_dup']}")
    print(f"duplicate but not code-flagged : {result['dup_not_code_flagged']}"
          "   <- what the backstop adds")
    print()
    print(result["report"].to_string(index=False))

    # Assertions, so a regression fails loudly rather than printing plausible
    # numbers.
    dup = find_duplicate_prints(df, size_col="size", price_col="price",
                                exchange_col="exchange",
                                time_col="trade_timestamp")
    flagged_100 = dup.loc[df["size"] == 100, "dup_repeat"].any()
    assert not flagged_100, "round-lot repetition was flagged — threshold broken"

    closing = dup.loc[df["size"] == 186_344, "dup_repeat"].sum()
    assert closing == 4, f"expected 4 closing re-reports flagged, got {closing}"

    backstop = dup.loc[df["size"] == 50_000, "dup_repeat"].sum()
    assert backstop == 1, f"expected the uncoded duplicate flagged, got {backstop}"

    # Same-instant opening pair: 62 and 66 share a timestamp, so the duplicate
    # guard must NOT fire — only the condition code catches that one.
    opening = dup.loc[df["size"] == 20_056, "dup_repeat"].sum()
    assert opening == 0, "same-instant prints should not trip the time guard"
    by_code = flag_restatement_codes(df, ["condition", "ext_condition1"])
    assert by_code.loc[df["condition"] == 66].all(), "code 66 not caught"

    print()
    print("All assertions passed.")
    print()
    print("Note the division of labour: the opening restatement (code 66) shares")
    print("a timestamp with the genuine print, so the time-based guard cannot")
    print("see it and the condition code must. The uncoded 50,000-share")
    print("duplicate is invisible to the codes and only the guard catches it.")
    print("Neither layer is sufficient alone.")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="Trade-tape quality guards")
    ap.add_argument("--selftest", action="store_true",
                    help="run the synthetic-tape checks (no network)")
    args = ap.parse_args()
    if args.selftest:
        _selftest()
    else:
        ap.print_help()
