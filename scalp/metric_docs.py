"""Metric name -> its definition and its section in METRICS.md.

Exists so the dashboard can make every column header a link. Looking at
`noise_bps_tw_mid_10s` should be one click from learning it is a median of
duration-weighted midpoints diffed across fixed 10-second buckets, with a
bucket-straddle approximation.

Anchors match the section ids in the published METRICS review page, so a
header link lands on the right section rather than the top of the document.

Names are resolved by exact match first, then by pattern, so the fifteen
generated `noise_bps_<variant>_<h>s` and `ratio_<variant>_<h>s` columns do not
each need an entry.
"""
from __future__ import annotations

import re

# Set to the published artifact URL, or a relative path if METRICS.md is
# served alongside the dashboard.
DOC_BASE = "https://claude.ai/code/artifact/e560e360-1128-4c36-8485-a8f7631adce6"

SECTIONS = {
    "source":     "The source and its limits",
    "resolution": "Quote resolution",
    "windows":    "Windows and exclusions",
    "collapse":   "Same-timestamp collapsing",
    "spread":     "Spread",
    "noise":      "Noise",
    "flicker":    "Flicker",
    "flow":       "Flow",
    "ranking":    "Ranking ratios and floors",
    "storage":    "Storage",
}

# metric -> (section anchor, one-line definition)
EXACT: dict[str, tuple[str, str]] = {
    # spread
    "spread_cents_mean":   ("spread", "Mean quoted spread, cents."),
    "spread_cents_median": ("spread", "Median quoted spread, cents."),
    "spread_bps_mean":     ("spread", "Mean quoted spread, bps of mid."),
    "spread_bps_median":   ("spread", "Median quoted spread, bps of mid."),
    "spread_cents_tw":     ("spread", "Spread in cents, weighted by how long each quote stood."),
    "spread_bps_tw":       ("spread", "Spread in bps, duration-weighted. Numerator of every ranking ratio."),
    "crossed_locked_share": ("spread", "Share of quotes with ask <= bid. Dropped from the spread stats."),
    "quote_observations":  ("spread", "Usable quote observations in the window."),

    # flicker (tick quote stream)
    "quote_records_per_min": ("flicker", "Raw quote records per minute. Genuine at tick, meaningless at 1s."),
    "nbbo_changes_per_min":  ("flicker", "Records where the bid or ask price actually moved, per minute."),
    "bid_changes_per_min":   ("flicker", "Bid price moves per minute."),
    "ask_changes_per_min":   ("flicker", "Ask price moves per minute."),
    "two_sided_change_share": ("flicker", "Both sides moved, as a share of any move. Measured at 1 in 10,278 on FDX."),
    "same_instant_share":    ("flicker", "Share of records sharing a timestamp with another. ~49.8%."),
    "bid_lifetime_ms_median": ("flicker", "Median time a best bid stood before being replaced. From the tick quote stream."),
    "ask_lifetime_ms_median": ("flicker", "Median time a best offer stood before being replaced."),
    "bid_lifetime_ms_mean":  ("flicker", "Mean best-bid lifetime, ms."),
    "ask_lifetime_ms_mean":  ("flicker", "Mean best-offer lifetime, ms."),
    "bbo_change_without_trade_share": ("flicker", "Cancel vs consumption. NOT IMPLEMENTED — needs both sources joined."),
    "quotes_per_trade":      ("flicker", "Quote records divided by trades. Ranking candidate; high churn per execution."),

    # flow
    "trades_per_min":     ("flow", "Trade arrivals per minute, after excluded prints."),
    "shares_per_min":     ("flow", "Shares per minute, after excluded prints."),
    "trade_size_mean":    ("flow", "Mean trade size."),
    "trade_size_median":  ("flow", "Median trade size."),
    "odd_lot_share":      ("flow", "Share below the PRICE-TIERED round lot (100/40/10/1 by price)."),
    "sub_100_share":      ("flow", "Share below a fixed 100 shares. Kept for comparability only."),
    "round_lot_size":     ("flow", "Round lot in force for this symbol-day, from the reference price."),
    "reference_price":    ("flow", "Median trade price, used to pick the round-lot tier."),
    "odd_lot_flag_disagree_share": ("flow", "Where the tiered calculation and vendor condition 115 disagree."),
    "odd_lot_vendor_share": ("flow", "Share flagged odd-lot by vendor condition 115."),
    "at_bid_share":       ("flow", "Trades at or below the bid."),
    "at_ask_share":       ("flow", "Trades at or above the ask."),
    "between_share":      ("flow", "Trades inside the spread."),
    "two_sided_balance":  ("flow", "min(at_bid, at_ask) / max(...). 1.0 balanced, 0.0 one-way."),
    "off_exchange_share": ("flow", "Share printed to a TRF or the ADF (codes 2, 57, 58, 59)."),
    "unidentified_exchange_share": ("flow", "Share on exchange code 78, absent from the vendor enum."),
    "off_mid_bps":        ("flow", "Median |price - mid| in bps. Ranking candidate."),

    # bookkeeping
    "rows_raw":        ("windows", "Records in the window before any exclusion."),
    "rows_excluded":   ("windows", "Records dropped by condition code."),
    "excluded_share":  ("windows", "Share of records dropped by condition code."),
    "window_minutes":  ("windows", "Length of the window the metrics cover."),
}

# (pattern, section, template) — the generated families.
PATTERNS: list[tuple[re.Pattern, str, str]] = [
    (re.compile(r"^noise_bps_(?P<v>tw_mid|last_mid|trade_price|bid_side|ask_side)_(?P<h>\d+)s$"),
     "noise",
     "MEDIAN absolute change in {vlabel} between consecutive fixed {h}-second "
     "buckets, in bps. Collapses to exactly 0 on sparse-quote names where "
     "over half the buckets are unchanged — prefer _mean/_p75/_p90/_rms "
     "there. Bucket-straddle approximation applies."),
    (re.compile(r"^noise_bps_(?P<v>tw_mid|last_mid|trade_price|bid_side|ask_side)_(?P<h>\d+)s_(?P<stat>mean|p75|p90|rms)$"),
     "noise",
     "{statlabel} of the absolute change in {vlabel} between consecutive "
     "fixed {h}-second buckets, in bps. Does not collapse to zero on a "
     "sparse-quote name the way the median does."),
    (re.compile(r"^move_rate_(?P<v>tw_mid|last_mid|trade_price|bid_side|ask_side)_(?P<h>\d+)s$"),
     "noise",
     "Share of consecutive {h}-second bucket pairs where {vlabel} changed at "
     "all — the HOW OFTEN half of the noise decomposition."),
    (re.compile(r"^move_bps_(?P<v>tw_mid|last_mid|trade_price|bid_side|ask_side)_(?P<h>\d+)s$"),
     "noise",
     "Median change in {vlabel} among the {h}-second bucket pairs that MOVED "
     "— the HOW FAR half, with the unchanged buckets taken out."),
    (re.compile(r"^zero_change_bucket_share_(?P<h>\d+)s$"),
     "noise",
     "Share of consecutive {h}-second buckets where the midpoint did not move "
     "at all. A direct measure of quote staleness, and a candidate signal in "
     "its own right — a book that is not moving is one where nothing is "
     "arriving."),
    (re.compile(r"^quote_bucket_coverage_(?P<h>\d+)s$"),
     "noise",
     "Buckets holding a quote observation over buckets in the window, at "
     "{h}s. NEU 0.59 against AAPL 0.995 separates sparse from dense quoting."),
    (re.compile(r"^ratio_(?P<v>tw_mid|last_mid|trade_price|bid_side|ask_side)_(?P<h>\d+)s(?:_(?P<stat>mean|p75|p90|rms))?$"),
     "ranking",
     "spread_bps_tw divided by {vlabel} noise at {h}s. One of the candidate "
     "ranking metrics; none is privileged until calibration."),
    (re.compile(r"^dropped_condition_(?P<code>\d+)$"),
     "windows",
     "Trades dropped for carrying condition code {code}."),

    # --- quiet windows ------------------------------------------------------
    # Trades only. Quotes are excluded by construction: a 29-share bid pulled
    # on EXPE moved the midpoint 19 cents in 30 seconds while the stock did
    # not move, and every quote-derived metric inherits that.
    (re.compile(r"^quiet_windows_(?P<w>\d+)s_(?P<t>05|10|20)$"),
     "noise",
     "Count of {w}-second windows where the level shift was under {tlabel} of "
     "the interquartile range of trade prices. Windows step every 10s and so "
     "OVERLAP — one quiet patch produces about {w}/10 of them, which is why "
     "quiet_episodes is the count of separate chances and this is not."),
    (re.compile(r"^quiet_eligible_windows_(?P<w>\d+)s$"),
     "noise",
     "Count of {w}-second windows with enough trades to measure — this one "
     "and its predecessor both at or above the guard, since the shift needs "
     "both levels. The denominator for every quiet count at {w}s: without it "
     "a count of zero cannot be told from never having had enough trades."),
    (re.compile(r"^quiet_episodes_(?P<w>\d+)s_(?P<t>05|10|20)$"),
     "noise",
     "Count of SEPARATE quiet patches at {w}s and {tlabel} — maximal runs of "
     "consecutive quiet windows, so overlapping windows collapse to one "
     "episode. The operational metric: only one name can be traded at a time, "
     "so what matters is how many distinct chances arrived, not how long they "
     "totalled."),
    (re.compile(r"^shift_over_range_median_(?P<w>\d+)s$"),
     "noise",
     "Median of shift over range across every eligible {w}-second window in "
     "the session, quiet or not. The continuous form of the same quantity the "
     "quiet counts threshold, and the pre-registered statistical test — it "
     "uses the whole distribution, so it has more power on a small sample of "
     "realised results."),
    (re.compile(r"^quiet_range_iqr_cents_(?P<w>\d+)s$"),
     "noise",
     "Median interquartile range of trade prices, in CENTS, across {w}-second "
     "windows that were quiet at the 1.0 threshold. Cents rather than bps "
     "because size is set by what the book absorbs, not by capital: profit is "
     "range-in-cents times shares available, and a bps figure penalises an "
     "expensive name for an expense that is not the binding constraint."),
    (re.compile(r"^quiet_range_iqr_bps_(?P<w>\d+)s$"),
     "noise",
     "The same interquartile range in bps of the level. Kept beside the cents "
     "figure because the two rank names differently, and which one is right "
     "depends on whether capital or liquidity is binding."),
    (re.compile(r"^quiet_range_p10p90_cents_(?P<w>\d+)s$"),
     "noise",
     "Median 10th-to-90th percentile span of trade prices, in CENTS, across "
     "quiet {w}-second windows. The middle 80% rather than the middle 50%, "
     "stored beside the IQR to test whether the IQR understates the area "
     "actually worked. Caution at {w}=15: near the 10-trade guard a single odd "
     "lot is 10% of the sample and lands on the p90 boundary, so a wide value "
     "there can be one stray print rather than a wide market."),
    (re.compile(r"^quiet_range_p10p90_bps_(?P<w>\d+)s$"),
     "noise",
     "The 10th-to-90th percentile span in bps of the level."),
    (re.compile(r"^shift_over_p10p90_median_(?P<w>\d+)s$"),
     "noise",
     "Median of shift over the 10-90 percentile span across every "
     "eligible {w}-second window. The twin of shift_over_range_median "
     "against the wider denominator, so if p10p90 wins the range "
     "comparison, whether the RATIO should move to it is answerable from "
     "the same recompute. Drives no counts and no thresholds."),
    (re.compile(r"^quiet_dollar_vol_per_min_(?P<w>\d+)s$"),
     "noise",
     "Median dollar volume per minute inside quiet {w}-second windows. Per "
     "minute rather than per window so the three window lengths compare."),
]

# Ratio thresholds, spelled out. 1.0 is the pre-registered primary: the shift
# equal to the range is the case where the old bid becomes the new ask — one
# loss in a string of winners, and the boundary of what is tradeable.
THRESHOLD_LABELS = {
    "05": "half",
    "10": "1.0x (the old bid becoming the new ask)",
    "20": "2.0x",
}

STAT_LABELS = {
    "mean": "Mean",
    "p75":  "75th percentile",
    "p90":  "90th percentile",
    "rms":  "Root mean square",
}

VARIANT_LABELS = {
    "tw_mid":      "the duration-weighted midpoint",
    "last_mid":    "the last midpoint in each bucket",
    "trade_price": "the last trade price in each bucket",
    "bid_side":    "the duration-weighted bid",
    "ask_side":    "the duration-weighted ask",
}


def describe(metric: str) -> tuple[str, str] | None:
    """(section anchor, one-line definition) for a metric, or None."""
    if metric in EXACT:
        return EXACT[metric]
    for pattern, section, template in PATTERNS:
        m = pattern.match(metric)
        if not m:
            continue
        parts = m.groupdict()
        if "v" in parts:
            parts["vlabel"] = VARIANT_LABELS.get(parts["v"], parts["v"])
        if parts.get("stat"):
            parts["statlabel"] = STAT_LABELS.get(parts["stat"], parts["stat"])
        if parts.get("t"):
            parts["tlabel"] = THRESHOLD_LABELS.get(parts["t"], parts["t"])
        return section, template.format(**parts)
    return None


def doc_url(metric: str) -> str | None:
    """Deep link to the metric's section, for a dashboard column header."""
    found = describe(metric)
    if not found:
        return None
    return f"{DOC_BASE}#{found[0]}"


def header_link(metric: str) -> dict:
    """Everything a dashboard header needs: label, tooltip, href.

    `href` is None for a metric with no entry — the dashboard should render a
    plain header rather than a dead link, and an unlinked column is a signal
    that a metric was added without being documented.
    """
    found = describe(metric)
    return {
        "metric": metric,
        "label": metric,
        "tooltip": found[1] if found else None,
        "href": doc_url(metric),
        "section": SECTIONS.get(found[0]) if found else None,
    }


def undocumented(metrics: list[str]) -> list[str]:
    """Metrics with no definition. Run this in CI or after adding a metric."""
    return [m for m in metrics if describe(m) is None]
