"""
Exit-rule registry and vectorised evaluator for build_trade_paths.py.

One function per rule, one registry entry per parameter value. Adding a rule
later is a new entry plus (usually) no new code, and the SAME evaluator serves
both the cached grid and any ad-hoc rule — there is deliberately no second
implementation that could disagree with this one.

--- Why this is fast -------------------------------------------------------

Naively, 59 rules x 450k paths x 7,800 bars is ~200 billion comparisons. It is
nothing like that, because almost every rule is a threshold crossing on a
MONOTONE running extremum:

    a long stop at level L fires at the first bar where cummin(low) <= L

cummin is non-increasing, so "first bar at or below L" is a binary search, not
a scan. All 7 fixed_stop levels, all 5 atr_stop levels and all 3 swing_low
levels therefore share ONE cummin(low) pass and cost a searchsorted each.
Targets share one cummax(high). Trailing stops share a cummin of
low/cummax(high).

Rules declare what they consume via `needs`; the evaluator computes each
feature once per block and passes it in. That is what keeps "one function per
rule" from meaning "one full pass per rule".

The activation-gated rules ("first crossing at or after bar t") look inherently
sequential but vectorise as a reverse minimum.accumulate over masked indices —
see _first_at_or_after. Nothing here needs Numba.

--- Fills: the gap-through rule --------------------------------------------

A stop does not fill at its level when the bar opens through it. Every rule
returns a trigger bar and the level in force at that bar; a single shared
resolver converts that to a fill:

    stop    fill = min(open[i], level)     gapped down -> filled at the open
    target  fill = max(open[i], level)     gapped up   -> filled at the open
    close   fill = close[i]                time and trend exits

This is why exit_return is stored rather than derived downstream from the
level: on a multi-day hold the difference between the level and the actual
open is the single largest source of error, and it is not recoverable from the
level alone.

--- Conventions ------------------------------------------------------------

Long only. Every rule assumes a long position entered at `entry_price`; a
short book would be a `direction` column and a sign flip, not new rules.

Day counting matches the _oc forward returns in daily_features:
max_days = N exits at the close of the (N-1)th session AFTER entry, so
max_days = 1 is a same-day exit and equals ret_1d_fwd_oc.

Same-bar ties are resolved in the COMBINE step, not here: each rule column is
independent, so when a stop and a target trigger on the same bar the tie is
broken by side priority (stop first). See build_combine_sql. At one-minute
granularity this is far rarer than it would be on daily bars, but it is still
a deliberate conservative bias.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

# Sentinel for "this rule never fired within the path horizon".
NEVER = -1

# Fill modes.
FILL_STOP   = "stop"
FILL_TARGET = "target"
FILL_CLOSE  = "close"

# Side priority for same-bar tie-breaking in the combine step. Lower wins.
SIDE_PRIORITY = {"stop": 0, "target": 1, "trend": 2, "time": 3}


@dataclass(frozen=True)
class Rule:
    key: str                 # 'fixed_stop__1p0' — also the column suffix
    family: str              # 'fixed_stop'
    side: str                # stop | target | time | trend
    fill: str                # FILL_STOP | FILL_TARGET | FILL_CLOSE
    params: dict
    needs: tuple = ()
    fn: object = None        # fn(F, ctx, **params) -> (trigger_idx, level)

    @property
    def bar_col(self) -> str:
        return f"xb_{self.key}"

    @property
    def ret_col(self) -> str:
        return f"xr_{self.key}"


def _slug(v) -> str:
    """1.0 -> '1p0', 0.5 -> '0p5', 20 -> '20'. Deterministic, so the dashboard
    can rebuild a column name from a parameter without a lookup table."""
    if isinstance(v, float) and not v.is_integer():
        return str(v).replace(".", "p")
    return str(int(v))


# --- Shared vectorised primitives -------------------------------------------

def _first_at_or_before(cum: np.ndarray, level: np.ndarray,
                        decreasing: bool) -> np.ndarray:
    """First column index where a MONOTONE running extremum crosses `level`.

    cum is (E, B) and monotone along axis 1 — cummin(low) when decreasing,
    cummax(high) when not. Because it is monotone, the crossing point is a
    binary search per row rather than a scan; this is the whole reason the
    parameter grid is nearly free once the accumulate has been paid for.

    Returns NEVER where the level is never reached.
    """
    E, B = cum.shape
    out = np.full(E, NEVER, dtype=np.int64)
    if decreasing:
        # searchsorted needs ascending input; negate to get it.
        idx = np.array([np.searchsorted(-cum[e], -level[e], side="left")
                        for e in range(E)])
    else:
        idx = np.array([np.searchsorted(cum[e], level[e], side="left")
                        for e in range(E)])
    hit = idx < B
    out[hit] = idx[hit]
    return out


def _first_at_or_after(cond: np.ndarray) -> np.ndarray:
    """(E, B) bool -> (E, B) int: nxt[e, i] = first j >= i with cond[e, j],
    or B if none.

    The sequential-looking recurrence nxt[i] = i if cond[i] else nxt[i+1]
    vectorises as a reverse running minimum over masked indices. This is what
    lets the 16 trail x activation combinations come from 4 scans instead of
    16 passes.
    """
    B = cond.shape[1]
    idx = np.where(cond, np.arange(B, dtype=np.int64)[None, :], B)
    return np.minimum.accumulate(idx[:, ::-1], axis=1)[:, ::-1]


# --- Feature computation ----------------------------------------------------

def compute_features(path: dict, needs: set, ctx: dict) -> dict:
    """Compute only the shared arrays the selected rules actually consume.

    path holds (E, B) float arrays: open, high, low, close — padded so that a
    bar beyond a path's real length can never trigger (low = +inf,
    high = -inf), which removes the need for a per-rule length mask.

    hwm_prev is the load-bearing one. It is the high-water mark THROUGH THE
    PRIOR BAR, floored at the entry price:

        hwm_prev[0] = entry_price
        hwm_prev[i] = max(entry_price, cummax(high)[i-1])

    Any rule whose level moves with price (trail, breakeven activation) must
    use this rather than the same-bar cummax. Using the same bar's high is
    incoherent with the gap-through resolver: it would set a trailing level
    from a high that had not happened yet when the bar opened, then treat the
    open as having gapped through that level. In practice that fired trailing
    and breakeven stops one bar early, at fills that were never available.

    Flooring at entry also gives the standard trailing-stop semantics — before
    price makes any new high, a `trail` percent trailing stop is simply a
    `trail` percent stop measured from entry.
    """
    F: dict = {}
    if "cummin_low" in needs:
        F["cummin_low"] = np.minimum.accumulate(path["low"], axis=1)
    if "cummax_high" in needs or "hwm_prev" in needs:
        F["cummax_high"] = np.maximum.accumulate(path["high"], axis=1)
    if "hwm_prev" in needs:
        entry = ctx["entry_price"][:, None]
        hwm_prev = np.empty_like(F["cummax_high"])
        hwm_prev[:, 0] = ctx["entry_price"]
        hwm_prev[:, 1:] = F["cummax_high"][:, :-1]
        F["hwm_prev"] = np.maximum(hwm_prev, entry)
        # low / resting high-water mark. A trailing stop at `trail` percent
        # fires when this ratio first falls to (1 - trail/100), so one
        # elementwise ratio serves every trail level.
        with np.errstate(divide="ignore", invalid="ignore"):
            F["trail_raw"] = path["low"] / F["hwm_prev"]
    return F


# --- Rule implementations ---------------------------------------------------
# Each returns (trigger_idx, level) where level is the price in force at the
# trigger bar. The shared resolver turns that into a fill.

def _rule_fixed_stop(F, ctx, pct):
    level = ctx["entry_price"] * (1.0 - pct / 100.0)
    return _first_at_or_before(F["cummin_low"], level, decreasing=True), level


def _rule_atr_stop(F, ctx, k):
    level = ctx["entry_price"] - k * ctx["atr"]
    return _first_at_or_before(F["cummin_low"], level, decreasing=True), level


def _rule_swing_low(F, ctx, n):
    level = ctx[f"swing_low_{n}"]
    return _first_at_or_before(F["cummin_low"], level, decreasing=True), level


def _rule_fixed_target(F, ctx, pct):
    level = ctx["entry_price"] * (1.0 + pct / 100.0)
    return _first_at_or_before(F["cummax_high"], level, decreasing=False), level


def _rule_atr_target(F, ctx, k):
    level = ctx["entry_price"] + k * ctx["atr"]
    return _first_at_or_before(F["cummax_high"], level, decreasing=False), level


def _activation_idx(F, ctx, act_pct):
    """First bar at which the position is ARMED — i.e. the first bar whose
    RESTING high-water mark (through the prior bar) has reached
    entry * (1 + act/100).

    Uses hwm_prev, not the same-bar cummax, for the same reason the trailing
    level does: arming on a high that occurs during the very bar that then
    stops you out invents an intrabar ordering the data does not contain.

    act = 0 arms at bar 0, which is what makes trail x activation=0 the plain
    trailing stop rather than a separate rule.
    """
    E, B = F["hwm_prev"].shape
    if act_pct == 0:
        return np.zeros(E, dtype=np.int64)
    level = ctx["entry_price"] * (1.0 + act_pct / 100.0)
    idx = _first_at_or_before(F["hwm_prev"], level, decreasing=False)
    # Never armed -> never fires; B is the "none" sentinel _first_at_or_after
    # uses, so map NEVER onto it.
    return np.where(idx == NEVER, B, idx)


def _gated_trigger(nxt, t_act):
    """First qualifying bar at or after the arming bar."""
    E, B = nxt.shape
    safe_t = np.clip(t_act, 0, B - 1)
    hit = nxt[np.arange(E), safe_t]
    hit = np.where(t_act >= B, B, hit)
    return np.where(hit >= B, NEVER, hit)


def _rule_trail(F, ctx, trail, activation):
    """Trailing stop, optionally armed only after a gain threshold.

    The stop level at bar i is hwm_prev[i] * (1 - trail/100) — the level
    actually resting when that bar opened. The high-water mark runs from ENTRY
    throughout; activation delays when the stop becomes live, it does not
    restart the HWM. Stated because the alternative (HWM measured from
    activation) is also defensible and silently changes results.
    """
    thresh = 1.0 - trail / 100.0
    cache = ctx.setdefault("_trail_nxt", {})
    if trail not in cache:
        cache[trail] = _first_at_or_after(F["trail_raw"] <= thresh)
    trigger = _gated_trigger(cache[trail], _activation_idx(F, ctx, activation))
    E, B = F["hwm_prev"].shape
    level = np.where(
        trigger >= 0,
        F["hwm_prev"][np.arange(E), np.clip(trigger, 0, B - 1)] * thresh,
        np.nan,
    )
    return trigger, level


def _rule_breakeven(F, ctx, activation):
    """Stop moves to entry once an activation gain is reached.

    A dip below entry BEFORE arming is not an exit — the stop was not there
    yet. That is why this gates on the arming bar rather than simply taking
    the first low below entry.
    """
    entry = ctx["entry_price"]
    if "_be_nxt" not in ctx:
        ctx["_be_nxt"] = _first_at_or_after(ctx["_low"] <= entry[:, None])
    trigger = _gated_trigger(ctx["_be_nxt"], _activation_idx(F, ctx, activation))
    return trigger, entry


def _rule_max_days(F, ctx, n):
    """Exit at the close of the (n-1)th session after entry.

    n = 1 is a same-day exit. This matches ret_1d_fwd_oc, which is the point:
    the max_days family is what makes trade_paths directly comparable to the
    _oc forward returns already in daily_features.
    """
    return ctx["session_end_idx"][:, n - 1].copy(), np.full(
        ctx["entry_price"].shape, np.nan)


def _rule_no_progress(F, ctx, gain_pct, day):
    """Exit at the close of session `day` if the gain there is under threshold.

    Evaluated only at that one session boundary — this is a give-up rule, not
    a continuously monitored one.
    """
    idx = ctx["session_end_idx"][:, day - 1]
    E = idx.shape[0]
    valid = idx >= 0
    close_at = np.full(E, np.nan)
    close_at[valid] = ctx["_close"][np.arange(E)[valid], idx[valid]]
    gain = close_at / ctx["entry_price"] - 1.0
    fired = valid & np.isfinite(gain) & (gain < gain_pct / 100.0)
    return np.where(fired, idx, NEVER), np.full(E, np.nan)


def _rule_ma_close_below(F, ctx, window):
    """First session close below its own trailing moving average.

    The MA is computed over the `window` sessions ENDING at that session
    inclusive, from the same price series as the path, so a constant vendor
    offset cancels on both sides of the comparison.
    """
    below = ctx[f"_ma_below_{window}"]     # (E, S) bool over session index
    sess_idx = ctx["session_end_idx"]      # (E, S) bar index of each close
    E, S = below.shape
    first = np.full(E, NEVER, dtype=np.int64)
    any_hit = below.any(axis=1)
    k = np.argmax(below, axis=1)
    first[any_hit] = sess_idx[np.arange(E)[any_hit], k[any_hit]]
    first = np.where(first < 0, NEVER, first)
    return first, np.full(E, np.nan)


# --- Registry ---------------------------------------------------------------

def _build_registry() -> list:
    R: list = []

    for pct in (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0):
        R.append(Rule(f"fixed_stop__{_slug(pct)}", "fixed_stop", "stop",
                      FILL_STOP, {"pct": pct}, ("cummin_low",),
                      _rule_fixed_stop))

    for k in (0.5, 1.0, 1.5, 2.0, 2.5):
        R.append(Rule(f"atr_stop__{_slug(k)}", "atr_stop", "stop",
                      FILL_STOP, {"k": k}, ("cummin_low",), _rule_atr_stop))

    for n in (1, 3, 5):
        R.append(Rule(f"swing_low__{n}", "swing_low", "stop",
                      FILL_STOP, {"n": n}, ("cummin_low",), _rule_swing_low))

    for trail in (1.0, 2.0, 3.0, 4.0):
        for act in (0.0, 1.0, 2.0, 3.0):
            R.append(Rule(
                f"trail__{_slug(trail)}_act{_slug(act)}", "trail", "stop",
                FILL_STOP, {"trail": trail, "activation": act},
                ("hwm_prev",), _rule_trail))

    for act in (1.0, 1.5, 2.0, 2.5):
        R.append(Rule(f"breakeven__{_slug(act)}", "breakeven", "stop",
                      FILL_STOP, {"activation": act}, ("hwm_prev",),
                      _rule_breakeven))

    for pct in (2, 3, 4, 5, 7, 10):
        R.append(Rule(f"fixed_target__{_slug(pct)}", "fixed_target", "target",
                      FILL_TARGET, {"pct": float(pct)}, ("cummax_high",),
                      _rule_fixed_target))

    for k in (1, 2, 3, 4, 5):
        R.append(Rule(f"atr_target__{_slug(k)}", "atr_target", "target",
                      FILL_TARGET, {"k": float(k)}, ("cummax_high",),
                      _rule_atr_target))

    for n in (1, 3, 5, 7, 10, 15, 20):
        R.append(Rule(f"max_days__{n}", "max_days", "time",
                      FILL_CLOSE, {"n": n}, (), _rule_max_days))

    for gain in (1.0, 2.0):
        for day in (2, 5):
            R.append(Rule(
                f"no_progress__{_slug(gain)}_d{day}", "no_progress", "time",
                FILL_CLOSE, {"gain_pct": gain, "day": day}, (),
                _rule_no_progress))

    for w in (10, 20):
        R.append(Rule(f"ma_close_below__{w}", "ma_close_below", "trend",
                      FILL_CLOSE, {"window": w}, (), _rule_ma_close_below))

    return R


REGISTRY: list = _build_registry()
BY_KEY: dict = {r.key: r for r in REGISTRY}

# The path horizon. Every path is computed to 20 sessions, so this rule always
# fires for a fully-resolved path — which is what makes it usable as a
# structural backstop in build_combine_sql rather than a convention the UI has
# to remember.
HORIZON_RULE_KEY = "max_days__20"
assert HORIZON_RULE_KEY in BY_KEY, "the horizon backstop must exist in the registry"

MAX_HORIZON_SESSIONS = 20


# --- Evaluation -------------------------------------------------------------

def resolve_fill(trigger, level, path, fill_mode, entry_price):
    """Trigger bar + level -> actual fill price and return.

    The gap-through rule lives here, once, for every rule:
      stop   -> min(open, level): a bar that opens BELOW the stop fills at the
                open, not the stop
      target -> max(open, level): a bar that opens ABOVE the target fills at
                the open
      close  -> the bar's close, for time and trend exits
    """
    E = trigger.shape[0]
    fill = np.full(E, np.nan)
    hit = trigger >= 0
    if not hit.any():
        return fill, fill.copy()
    rows = np.arange(E)[hit]
    cols = trigger[hit]
    o = path["open"][rows, cols]
    if fill_mode == FILL_STOP:
        fill[hit] = np.minimum(o, level[hit] if np.ndim(level) else level)
    elif fill_mode == FILL_TARGET:
        fill[hit] = np.maximum(o, level[hit] if np.ndim(level) else level)
    else:
        fill[hit] = path["close"][rows, cols]
    ret = fill / entry_price - 1.0
    return fill, ret


def evaluate(path: dict, ctx: dict, rules: list | None = None) -> dict:
    """Run rules over one block of paths.

    Returns {rule_key: (exit_bar int64 array, exit_return float array)} with
    exit_bar = NEVER where the rule did not fire inside the horizon.
    """
    rules = rules or REGISTRY
    needs: set = set()
    for r in rules:
        needs.update(r.needs)
    F = compute_features(path, needs, ctx)

    ctx = dict(ctx)
    ctx["_low"] = path["low"]
    ctx["_close"] = path["close"]

    out: dict = {}
    for r in rules:
        trigger, level = r.fn(F, ctx, **r.params)
        trigger = np.asarray(trigger, dtype=np.int64)
        # A trigger past the path's real length is not an exit. Padding makes
        # this nearly impossible, but a rule computing an index directly
        # (max_days on a truncated path) can still produce one.
        trigger = np.where(trigger >= ctx["path_len"], NEVER, trigger)
        _fill, ret = resolve_fill(trigger, level, path, r.fill,
                                  ctx["entry_price"])
        out[r.key] = (trigger, ret)
    return out


# --- Combine helper (what the dashboard calls) ------------------------------

class CombineError(ValueError):
    pass


def build_combine_sql(rule_keys, table: str = "trade_paths",
                      include_unresolved: bool = False) -> tuple:
    """SQL selecting the winning exit across `rule_keys`, plus metadata.

    THE HORIZON BACKSTOP IS STRUCTURAL, NOT A CONVENTION.

    Postgres's LEAST ignores NULLs, and a NULL exit_bar means "this rule never
    fired". So a policy whose stops all miss would yield LEAST(NULL, NULL) =
    NULL — a trade that never exits, which surfaces downstream as a
    plausible-looking return rather than an error. That is the failure mode
    this function exists to make impossible.

    HORIZON_RULE_KEY is therefore appended to every combine unconditionally.
    It is a no-op whenever any selected rule fires earlier (LEAST picks the
    smaller), and it is the guaranteed exit when none does. There is no code
    path through this function that produces an unbounded exit.

    Ties are broken by side priority — stop, then target, then trend, then
    time — implementing the documented same-bar convention that a stop is
    assumed to have fired first.

    Unresolved paths (path_status <> 'ok') are excluded by default. Those are
    entries whose horizon extends past the end of available data; their
    horizon exit is genuinely unknown, and including them would silently mix
    "not yet resolved" into realised statistics.
    """
    if not rule_keys:
        raise CombineError(
            "no rules selected: a combine with no rules has no exit at all. "
            "Select at least one rule; the horizon backstop "
            f"({HORIZON_RULE_KEY}) is added automatically but is not a policy."
        )
    unknown = [k for k in rule_keys if k not in BY_KEY]
    if unknown:
        raise CombineError(
            f"unknown rule key(s): {unknown}. Valid keys come from the "
            f"registry in lib/trade_path_rules.py ({len(BY_KEY)} rules)."
        )

    keys = list(dict.fromkeys(rule_keys))
    horizon_added = HORIZON_RULE_KEY not in keys
    if horizon_added:
        keys.append(HORIZON_RULE_KEY)

    # The backstop is the rule GUARANTEED to fire, and the resolution filter
    # below is written against its column, so it must never be shorter than a
    # selected time rule. If it were, the damage would not stop at the filter:
    # LEAST would return the backstop's earlier bar and silently truncate the
    # selection to the backstop's horizon, answering a different question than
    # the one asked. Unreachable while max_days__20 is the longest rule in the
    # catalog -- it becomes reachable the moment a longer horizon is added,
    # which is exactly when it must fail loudly instead of quietly.
    h_n = BY_KEY[HORIZON_RULE_KEY].params["n"]
    longer = [k for k in keys if BY_KEY[k].family == "max_days"
              and BY_KEY[k].params["n"] > h_n]
    if longer:
        raise CombineError(
            f"selected time rule(s) {longer} run past the horizon backstop "
            f"{HORIZON_RULE_KEY} ({h_n} sessions). LEAST would truncate them "
            f"to the backstop's exit, and the resolution filter would "
            f"understate the sessions they need. Make the backstop a function "
            f"of the selection before adding horizons longer than {h_n}."
        )

    # Stop before target before trend before time, so a same-bar tie resolves
    # to the stop. Within a side, order is stable on the caller's selection.
    ordered = sorted(keys, key=lambda k: SIDE_PRIORITY[BY_KEY[k].side])

    bar_cols = [BY_KEY[k].bar_col for k in keys]
    least = "LEAST(" + ", ".join(bar_cols) + ")"

    cases = "\n".join(
        f"        WHEN {BY_KEY[k].bar_col} = w.exit_bar THEN {BY_KEY[k].ret_col}"
        for k in ordered
    )
    # Resolution is per-rule data, not a table-wide flag.
    #
    # path_status is a GLOBAL boolean, stamped by build_trade_paths against the
    # build's single longest horizon (MAX_HORIZON_SESSIONS). Filtering on it
    # makes EVERY combine inherit that longest horizon's tail truncation: a
    # one-session policy is denied the same trailing entries as a twenty-
    # session one, for a reason that has nothing to do with the one session it
    # actually needs. Extending the catalog's horizon would therefore silently
    # shrink the eligible population of every existing combination -- including
    # the short ones -- which is the failure this filter exists to prevent.
    #
    # The horizon rule's own exit_bar already carries the same fact per
    # horizon: it is NULL exactly when that rule's final session was not
    # reachable in the available data.
    #
    # This is EQUIVALENT to path_status = 'ok' while max_days__20 is the
    # longest rule in the catalog, and deliberately so. build_trade_paths
    # stamps `full` from (si + H - 1) <= last_session, which is the same
    # predicate that sets sess_end_rel[:, H-1] to NEVER for the horizon rule.
    # Test 13 verifies that equivalence against the build's own arithmetic
    # rather than assuming it.
    backstop_col = BY_KEY[HORIZON_RULE_KEY].bar_col
    where = ("" if include_unresolved
             else f"\n    WHERE {backstop_col} IS NOT NULL")

    sql = (
        f"WITH w AS (\n"
        f"    SELECT ticker, trade_date, entry_anchor,\n"
        f"           {least} AS exit_bar,\n"
        f"           {', '.join(bar_cols + [BY_KEY[k].ret_col for k in keys])}\n"
        f"    FROM {table}{where}\n"
        f")\n"
        f"SELECT ticker, trade_date, entry_anchor, exit_bar,\n"
        f"    CASE\n{cases}\n"
        f"    END AS exit_return\n"
        f"FROM w"
    )

    meta = {
        "rules": keys,
        "horizon_rule": HORIZON_RULE_KEY,
        "horizon_auto_added": horizon_added,
        "tie_break_order": [BY_KEY[k].side for k in ordered],
        "excludes_unresolved": not include_unresolved,
        "backstop_rule": HORIZON_RULE_KEY,
        "resolution_column": backstop_col,
    }
    return sql, meta


def registry_rows() -> list:
    """Registry as plain dicts, for the trade_path_rules catalog table so the
    dashboard builds column names from data rather than hardcoding 118 of
    them."""
    import json
    return [{
        "rule_key": r.key,
        "family": r.family,
        "side": r.side,
        "fill_mode": r.fill,
        "params": json.dumps(r.params),
        "exit_bar_col": r.bar_col,
        "exit_return_col": r.ret_col,
        "is_horizon": r.key == HORIZON_RULE_KEY,
    } for r in REGISTRY]
