"""
Dynamic schema sync for wf_bins and is_bins.

The set of columns in each bin table is driven by metric_classification,
filtered to eligible_as_metric = TRUE.  For every eligible metric we ensure
two columns exist in each table:
    frac_<metric>   DOUBLE PRECISION  (nullable; NULL when value absent)
    bin20_<metric>  SMALLINT NOT NULL DEFAULT 0  (0 when value absent)

Idempotent.  Never auto-drops columns when a metric becomes ineligible —
orphan columns from deprecated metrics are harmless and a destructive
auto-drop would be a footgun.  Manual ALTER TABLE DROP COLUMN when intended.

Called from:
    init_db.py                  — deploy-time, after sql/05_bin_tables.sql runs
    build_bin_tables.py startup — runtime, catches metrics added between deploys
"""
from __future__ import annotations

import logging
import re

log = logging.getLogger(__name__)

# Postgres identifier safety check.  Metric names come from the database, not
# user input, but defense-in-depth: refuse to ALTER TABLE with anything that
# isn't a clean lowercase-snake identifier.
_VALID_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _validate_metric_name(name: str) -> None:
    if not _VALID_IDENT_RE.match(name):
        raise ValueError(
            f"Refusing to build column for unsafe metric name: {name!r}. "
            "Expected lowercase snake_case identifier."
        )


# Reads ----------------------------------------------------------------------

def get_eligible_metrics(conn) -> list:
    """Return list of (metric, tier) tuples for eligible metrics, sorted by metric.

    Reads metric_classification WHERE eligible_as_metric = TRUE.  tier values
    are MORNING / EVENING.  (Spec confirmed: tier = 'both' rows are all
    eligible_as_metric = FALSE — they would be filtered out before reaching
    callers of this function.)
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT metric, tier FROM metric_classification "
            "WHERE eligible_as_metric = TRUE "
            "ORDER BY metric"
        )
        return cur.fetchall()


def get_metrics_by_tier(conn) -> dict:
    """Return {'MORNING': [...], 'EVENING': [...]} of eligible metric names."""
    rows = get_eligible_metrics(conn)
    out = {"MORNING": [], "EVENING": []}
    for metric, tier in rows:
        tier_norm = (tier or "").strip().upper()
        if tier_norm not in ("MORNING", "EVENING"):
            log.warning(
                "Metric %s has unexpected tier %r; the eligibility filter "
                "should have excluded 'both' but this is neither MORNING nor "
                "EVENING — skipping.",
                metric, tier,
            )
            continue
        out[tier_norm].append(metric)
    return out


def existing_wf_bins_columns(conn) -> set:
    """Set of column names currently in wf_bins."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'wf_bins' AND table_schema = 'public'"
        )
        return {r[0] for r in cur.fetchall()}


def existing_is_bins_columns(conn) -> set:
    """Set of column names currently in is_bins."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'is_bins' AND table_schema = 'public'"
        )
        return {r[0] for r in cur.fetchall()}


def existing_tt_bins_columns(conn) -> set:
    """Set of column names currently in tt_bins."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'tt_bins' AND table_schema = 'public'"
        )
        return {r[0] for r in cur.fetchall()}


def existing_daily_features_columns(conn) -> set:
    """Set of column names in daily_features.  Used to defensively filter the
    eligible-metric list to only those that exist as columns in the source
    table — guards against drift between metric_classification and the actual
    schema."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'daily_features' AND table_schema = 'public'"
        )
        return {r[0] for r in cur.fetchall()}


# Sync -----------------------------------------------------------------------

def sync_wf_bins_schema(conn) -> tuple:
    """Add missing (frac_<m>, bin20_<m>) column pairs for every eligible metric.

    Returns (added_pair_count, added_metric_names).  Idempotent.  Never drops.
    """
    existing = existing_wf_bins_columns(conn)
    df_cols = existing_daily_features_columns(conn)
    eligible = get_eligible_metrics(conn)

    added: list = []
    skipped_missing: list = []

    with conn.cursor() as cur:
        for metric, _tier in eligible:
            if metric not in df_cols:
                skipped_missing.append(metric)
                continue
            _validate_metric_name(metric)
            frac_col = f"frac_{metric}"
            bin_col = f"bin20_{metric}"
            need_frac = frac_col not in existing
            need_bin = bin_col not in existing
            if need_frac:
                cur.execute(
                    f'ALTER TABLE wf_bins ADD COLUMN IF NOT EXISTS {frac_col} '
                    f'DOUBLE PRECISION'
                )
            if need_bin:
                cur.execute(
                    f'ALTER TABLE wf_bins ADD COLUMN IF NOT EXISTS {bin_col} '
                    f'SMALLINT NOT NULL DEFAULT 0'
                )
            if need_frac or need_bin:
                added.append(metric)
    conn.commit()

    if skipped_missing:
        log.warning(
            "Skipped %d metric(s) listed in metric_classification but absent "
            "from daily_features (drift between catalog and schema): %s",
            len(skipped_missing), skipped_missing[:10],
        )
    if added:
        log.info("Added wf_bins column pairs for %d metric(s): %s",
                 len(added), added[:10])
    return len(added), added


def sync_is_bins_schema(conn) -> tuple:
    """Add missing (frac_<m>, bin20_<m>) column pairs to is_bins for every
    eligible metric.  Exact mirror of sync_wf_bins_schema targeting is_bins.

    Returns (added_pair_count, added_metric_names).  Idempotent.  Never drops.
    """
    existing = existing_is_bins_columns(conn)
    df_cols  = existing_daily_features_columns(conn)
    eligible = get_eligible_metrics(conn)

    added:           list = []
    skipped_missing: list = []

    with conn.cursor() as cur:
        for metric, _tier in eligible:
            if metric not in df_cols:
                skipped_missing.append(metric)
                continue
            _validate_metric_name(metric)
            frac_col = f"frac_{metric}"
            bin_col  = f"bin20_{metric}"
            need_frac = frac_col not in existing
            need_bin  = bin_col  not in existing
            if need_frac:
                cur.execute(
                    f'ALTER TABLE is_bins ADD COLUMN IF NOT EXISTS {frac_col} '
                    f'DOUBLE PRECISION'
                )
            if need_bin:
                cur.execute(
                    f'ALTER TABLE is_bins ADD COLUMN IF NOT EXISTS {bin_col} '
                    f'SMALLINT NOT NULL DEFAULT 0'
                )
            if need_frac or need_bin:
                added.append(metric)
    conn.commit()

    if skipped_missing:
        log.warning(
            "Skipped %d metric(s) listed in metric_classification but absent "
            "from daily_features (drift between catalog and schema): %s",
            len(skipped_missing), skipped_missing[:10],
        )
    if added:
        log.info("Added is_bins column pairs for %d metric(s): %s",
                 len(added), added[:10])
    return len(added), added


def sync_tt_bins_schema(conn) -> tuple:
    """Add missing bin20_<m> columns to tt_bins for every eligible metric.

    UNLIKE sync_wf_bins_schema / sync_is_bins_schema, which add a frac+bin20
    PAIR per metric, tt_bins stores ONLY bin20 — there is no frac_<metric>
    column.  Bin5 and bin10 derive at read time from bin20 via integer
    division (bin10 = (bin20-1)//2+1, bin5 = (bin20-1)//4+1).

    Returns (added_count, added_metric_names).  Idempotent.  Never drops.
    """
    existing = existing_tt_bins_columns(conn)
    df_cols  = existing_daily_features_columns(conn)
    eligible = get_eligible_metrics(conn)

    added:           list = []
    skipped_missing: list = []

    with conn.cursor() as cur:
        for metric, _tier in eligible:
            if metric not in df_cols:
                skipped_missing.append(metric)
                continue
            _validate_metric_name(metric)
            bin_col = f"bin20_{metric}"
            if bin_col not in existing:
                cur.execute(
                    f'ALTER TABLE tt_bins ADD COLUMN IF NOT EXISTS {bin_col} '
                    f'SMALLINT NOT NULL DEFAULT 0'
                )
                added.append(metric)
    conn.commit()

    if skipped_missing:
        log.warning(
            "Skipped %d metric(s) listed in metric_classification but absent "
            "from daily_features (drift between catalog and schema): %s",
            len(skipped_missing), skipped_missing[:10],
        )
    if added:
        log.info("Added tt_bins bin20 columns for %d metric(s): %s",
                 len(added), added[:10])
    return len(added), added
