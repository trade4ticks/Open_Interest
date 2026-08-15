"""
Symbol-history resolution for tickers that were renamed.

The failure this exists to prevent
---------------------------------
META was FB until 2022-06-09. Polygon stores trades under the symbol that was
in force at the time, so requesting META for 2019 returns an empty result — not
an error. Every layer downstream then behaves *correctly* and the data is still
wrong: the fetcher records the chunk as `empty`, the completeness audit accepts
an absence it has no way to distinguish from a pre-listing gap, and the store
ends up with three years of silent absence in a name that traded throughout.

No amount of care in the fetcher or the audit catches this, because nothing in
either has the information needed to know the absence is wrong. The only fix is
to ask the vendor what the symbol actually was on each date.

Resolution
----------
/vX/reference/tickers/{id}/events returns the dates on which each symbol became
active for the entity currently trading under the queried ticker:

    2022-06-09 -> META
    2012-05-18 -> FB

So the symbol in force on date D is the entry with the greatest date <= D, and
the history becomes a list of half-open intervals. A fetch range that straddles
a rename is split at the boundary and each part requested under its own symbol.

Degradation
-----------
The events endpoint is vendor-flagged experimental. Every failure path here
degrades to identity mapping (the ticker is its own history) rather than
aborting, because a backfill that stops on a reference-data hiccup is worse
than one that proceeds and is caught by the leading-empty report. Which is why
that report exists regardless of whether this module works: it is the
independent check, not a redundant one.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta

log = logging.getLogger(__name__)


CLASS_RENAME     = "RENAME"
CLASS_SPAC       = "LIKELY_SPAC_SHELL"
CLASS_BANKRUPTCY = "LIKELY_BANKRUPTCY"
CLASS_UNKNOWN    = "UNKNOWN"

# Names of SPAC shells before a de-SPAC. Matching on the FORMER entity's name
# is far more reliable than anything derivable from the symbol.
#
# The trailing-roman-numeral pattern earns its place: SPAC sponsors issue
# numbered series ("Churchill Capital Corp IV", "Social Capital Hedosophia
# Holdings Corp V"), and a roman numeral after "Corp" is close to a tell.
# Without it "Social Capital Hedosophia Holdings Corp V" reads as a plain
# rebrand, because it contains neither "Acquisition" nor "Capital Corp".
#
# Tuned to over-flag rather than under-flag. A false positive costs one line of
# human review; a false negative would silently splice a cash shell into a
# training window. Splicing is opt-in regardless, so neither is load-bearing.
_SPAC_NAME_RE = re.compile(
    r"\b(?:"
    r"acquisition"
    r"|blank\s+check"
    r"|(?:capital|holdings)\s+corp"
    r"|corp(?:oration)?\.?\s+(?:I{1,3}|IV|VI{0,3}|IX|X{1,3})\b"
    r")", re.I)


def classify_change(prior_symbol: str, prior_name: str,
                    prior_details: dict | None = None) -> str:
    """Best-effort label for what a ticker_change actually WAS.

    A `ticker_change` event is a single vendor concept covering things that are
    not remotely the same economic event:

      FB   -> META   a rebrand. Same company, same business, continuous price
                     series. Splicing is correct.
      CCIV -> LCID   a de-SPAC. Legally the same registrant — which is exactly
                     why the vendor reports it as a ticker_change — but the
                     pre-merger series is a cash shell trading on merger
                     rumour. As LCID price history it is actively misleading,
                     worse than absent.
      HTZ  -> HTZGQ -> HTZ   a bankruptcy delisting and relisting. The OTC
                     stub is a different security in all but name.

    This returns a HINT, never a decision. Splicing is opt-in per ticker
    regardless of what this says, because the classifier can only ever be
    heuristic and the cost of a wrong auto-splice is silent contamination of
    the training window.
    """
    details = prior_details or {}
    name = prior_name or details.get("name") or ""

    if _SPAC_NAME_RE.search(name):
        return CLASS_SPAC
    # Bankruptcy stubs carry a 5th-letter Q; a delisting date on the prior
    # symbol points the same way.
    if len(prior_symbol) == 5 and prior_symbol.upper().endswith("Q"):
        return CLASS_BANKRUPTCY
    if details.get("delisted_utc"):
        return CLASS_BANKRUPTCY
    if not name:
        return CLASS_UNKNOWN
    return CLASS_RENAME


class SymbolHistory:
    """Maps (canonical ticker, date) -> the symbol that traded on that date.

    `canonical` is the CURRENT symbol and is what the store keys on, so a
    rename never splits one company across two ticker directories. The symbol
    actually requested is carried separately as provenance.

    SPLICING IS OPT-IN. A discovered rename is reported but NOT used unless the
    ticker is explicitly allowed, because `ticker_change` conflates rebrands
    with de-SPACs and bankruptcy relistings (see classify_change). Silently
    splicing a SPAC shell would inject rumour-driven price action into the
    training window under the operating company's name — a defect that no
    completeness check can see, since the data is present and well-formed.

    The default therefore leaves a visible hole rather than inventing history.
    The leading-empty report is what surfaces that hole for a decision.
    """

    def __init__(self, canonical: str, intervals: list,
                 splice: bool = False, classification: str = CLASS_UNKNOWN,
                 prior_names: dict | None = None):
        # intervals: [(start_date, end_date_or_None, symbol)] ascending,
        # end_date inclusive, final interval open-ended (None).
        self.canonical = canonical.upper()
        self.intervals = intervals
        self.splice = splice
        self.classification = classification
        self.prior_names = prior_names or {}

    @property
    def renamed(self) -> bool:
        """True when the vendor reports a former symbol — regardless of whether
        splicing is enabled. Discovery and use are deliberately separate."""
        return len(self.intervals) > 1

    @property
    def former_symbols(self) -> list:
        return [s for _, _, s in self.intervals if s.upper() != self.canonical]

    def symbol_for(self, d: date) -> str:
        for start, end, sym in self.intervals:
            if d >= start and (end is None or d <= end):
                return sym
        # Before the earliest recorded event: the vendor has no symbol for this
        # date, so the oldest known one is the best available guess. It will
        # return empty if wrong, which the leading-empty report then surfaces.
        return self.intervals[0][2] if self.intervals else self.canonical

    def split_range(self, start: date, end: date) -> list:
        """[(sub_start, sub_end, symbol)] covering [start, end] with one symbol
        each. Returns a single span for the overwhelmingly common no-rename
        case, so the caller's fast path stays one request.

        With splicing disabled (the default) this ALWAYS returns the canonical
        symbol, so a renamed ticker fetches exactly as it would have without
        any symbol history — an honest hole rather than a silent splice.
        """
        if not self.renamed or not self.splice:
            return [(start, end, self.canonical)]
        out: list = []
        for i_start, i_end, sym in self.intervals:
            a = max(start, i_start)
            b = end if i_end is None else min(end, i_end)
            if a <= b:
                out.append((a, b, sym))
        return out or [(start, end, self.canonical)]

    def describe(self) -> str:
        parts = []
        for start, end, sym in self.intervals:
            parts.append(f"{sym} {start}..{end or 'present'}")
        return " | ".join(parts)


def _parse_events(canonical: str, events: list) -> list:
    """Vendor events -> ascending inclusive intervals."""
    parsed: list = []
    for ev in events or []:
        if ev.get("type") != "ticker_change":
            continue
        raw_date = ev.get("date")
        sym = (ev.get("ticker_change") or {}).get("ticker")
        if not raw_date or not sym:
            continue
        try:
            d = datetime.strptime(str(raw_date)[:10], "%Y-%m-%d").date()
        except ValueError:
            log.warning("  %s: unparseable event date %r — ignored",
                        canonical, raw_date)
            continue
        parsed.append((d, sym.upper()))

    if not parsed:
        return [(date(1900, 1, 1), None, canonical.upper())]

    parsed.sort(key=lambda x: x[0])

    intervals: list = []
    for i, (d, sym) in enumerate(parsed):
        end = (parsed[i + 1][0] - timedelta(days=1)) if i + 1 < len(parsed) else None
        intervals.append((d, end, sym))

    # Extend the earliest interval backwards: a fetch range that starts before
    # the first recorded event still needs a symbol, and the oldest known one is
    # the only defensible choice.
    first_start, first_end, first_sym = intervals[0]
    intervals[0] = (date(1900, 1, 1), first_end, first_sym)
    return intervals


def build_symbol_history(ticker: str, fetch_events=None, fetch_details=None,
                         splice: bool = False) -> SymbolHistory:
    """Resolve one ticker's symbol history. Never raises.

    `splice` only controls whether the history is USED. Discovery happens
    either way so the caller can report what it found.

    When a former symbol is discovered, its reference record is looked up so
    the change can be classified — the former entity's NAME is what separates
    a rebrand from a de-SPAC, and a human reading "Churchill Capital Corp IV"
    next to "LCID" needs no further explanation.
    """
    if fetch_events is None:
        from lib.polygon import fetch_ticker_events as fetch_events
    try:
        events = fetch_events(ticker)
    except Exception as exc:
        log.warning("  %s: symbol history lookup failed (%s) — assuming no "
                    "rename. A long leading gap for this ticker in the "
                    "empty-prefix report is the signal that this was wrong.",
                    ticker, exc)
        events = []

    intervals = _parse_events(ticker, events)
    hist = SymbolHistory(ticker, intervals, splice=splice)
    if not hist.renamed:
        return hist

    if fetch_details is None:
        try:
            from lib.polygon import fetch_ticker_details as fetch_details
        except Exception:
            fetch_details = None

    worst = CLASS_RENAME
    for sym in hist.former_symbols:
        det = {}
        if fetch_details is not None:
            try:
                det = fetch_details(sym) or {}
            except Exception as exc:
                log.warning("  %s: details lookup failed for former symbol %s "
                            "— %s", ticker, sym, exc)
        name = det.get("name") or ""
        hist.prior_names[sym] = name
        cls = classify_change(sym, name, det)
        # Any non-rebrand signal dominates: one SPAC leg is enough to make the
        # whole splice unsafe.
        if cls in (CLASS_SPAC, CLASS_BANKRUPTCY):
            worst = cls
        elif cls == CLASS_UNKNOWN and worst == CLASS_RENAME:
            worst = CLASS_UNKNOWN
    hist.classification = worst
    return hist


def build_all(tickers: list, max_workers: int = 8,
              splice_allow: set | None = None) -> dict:
    """{ticker: SymbolHistory} for a universe, fetched concurrently.

    One cheap reference call per ticker — 121 calls against an 11,132-request
    backfill, so it is not worth optimising away, and doing it up front means a
    rename is known before any bar is requested.

    `splice_allow` is the explicit per-ticker opt-in. Anything not in it is
    discovered and reported but fetched under the canonical symbol only.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    allow = {t.upper() for t in (splice_allow or set())}
    out: dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = {pool.submit(build_symbol_history, t,
                            splice=(t.upper() in allow or "ALL" in allow)): t
                for t in tickers}
        for fut in as_completed(futs):
            t = futs[fut]
            try:
                out[t] = fut.result()
            except Exception as exc:
                log.warning("  %s: symbol history failed — %s", t, exc)
                out[t] = SymbolHistory(t, [(date(1900, 1, 1), None, t.upper())])
    return out


def report_symbol_histories(symbol_map: dict, out_csv: str | None = None) -> list:
    """Print every discovered rename with its classification and splice state.

    Prominence is the requirement here: a discovered-but-not-spliced rename is
    a decision waiting to be made, and it has to be impossible to miss in the
    run output.
    """
    import csv as _csv

    rows = []
    for t, h in sorted(symbol_map.items()):
        if not h.renamed:
            continue
        for sym in h.former_symbols:
            rows.append({
                "ticker": t,
                "former_symbol": sym,
                "former_name": h.prior_names.get(sym, ""),
                "classification": h.classification,
                "spliced": "yes" if h.splice else "NO",
                "history": h.describe(),
            })
    if not rows:
        return rows

    print(f"\n{'=' * 78}")
    print(f"SYMBOL CHANGES DISCOVERED — {len(rows)} across "
          f"{len({r['ticker'] for r in rows})} ticker(s)")
    print(f"{'=' * 78}")
    print(f"  {'ticker':<8}{'former':<8}{'spliced':<9}{'classification':<20}"
          f"former entity name")
    for r in sorted(rows, key=lambda x: (x["classification"], x["ticker"])):
        print(f"  {r['ticker']:<8}{r['former_symbol']:<8}{r['spliced']:<9}"
              f"{r['classification']:<20}{r['former_name'][:34]}")

    unspliced = [r for r in rows if r["spliced"] == "NO"]
    shells = [r for r in rows
              if r["classification"] in (CLASS_SPAC, CLASS_BANKRUPTCY)]
    if shells:
        print(f"\n  {len(shells)} change(s) look like a de-SPAC or bankruptcy "
              f"relisting, NOT a rebrand.")
        print("  The former symbol's price action is a different economic "
              "entity. Splicing it")
        print("  would inject shell/rumour pricing into this ticker's history "
              "under its own name.")
    if unspliced:
        print(f"\n  {len(unspliced)} change(s) were NOT spliced (the default). "
              f"Those tickers have")
        print("  no data before their rename date. To splice specific ones "
              "after reviewing:")
        print("    python fetch_equity_1min.py --repair --splice-renames "
              + ",".join(sorted({r["ticker"] for r in unspliced})[:4])
              + (" ..." if len({r['ticker'] for r in unspliced}) > 4 else ""))

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            w = _csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\n  CSV: {out_csv}")
    return rows
