"""
Dynamic per-rule column sync for trade_paths.

Mirrors lib/bin_schema.py: the skeleton table lives in SQL, and the per-rule
column pairs are added idempotently from a registry. That is what makes "add a
rule = write one function, re-run" true end to end — no hand-written ALTER
TABLE, and no chance of the table and the registry drifting apart.

Column types are deliberately narrow. exit_bar is SMALLINT: the widest path is
20 sessions x 390 regular bars = 7,800, and 19,200 even with extended hours,
both inside int2's 32,767. exit_return is REAL. Six bytes per rule x 59 rules
x 450k rows is ~160 MB; DOUBLE PRECISION and INTEGER would double that for
precision no return needs.
"""
from __future__ import annotations

import logging
import re

from lib.trade_path_rules import REGISTRY, registry_rows

log = logging.getLogger(__name__)

_SAFE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate(col: str) -> None:
    """Registry keys reach SQL by string interpolation, so they are validated
    rather than trusted. They are developer-authored, but a rule key with a
    quote in it would be an injection point and is trivially preventable."""
    if not _SAFE.match(col) or len(col) > 63:
        raise ValueError(f"unsafe or over-long column name from registry: {col!r}")


def existing_trade_paths_columns(conn) -> set:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'trade_paths' AND table_schema = 'public'"
        )
        return {r[0] for r in cur.fetchall()}


def sync_trade_paths_schema(conn) -> tuple:
    """Add missing (xb_<rule>, xr_<rule>) pairs. Idempotent. Never drops."""
    existing = existing_trade_paths_columns(conn)
    added: list = []
    with conn.cursor() as cur:
        for r in REGISTRY:
            _validate(r.bar_col)
            _validate(r.ret_col)
            need_b = r.bar_col not in existing
            need_r = r.ret_col not in existing
            if need_b:
                cur.execute(f"ALTER TABLE trade_paths "
                            f"ADD COLUMN IF NOT EXISTS {r.bar_col} SMALLINT")
            if need_r:
                cur.execute(f"ALTER TABLE trade_paths "
                            f"ADD COLUMN IF NOT EXISTS {r.ret_col} REAL")
            if need_b or need_r:
                added.append(r.key)
    conn.commit()
    if added:
        log.info("Added trade_paths column pairs for %d rule(s): %s",
                 len(added), added[:10])
    return len(added), added


def sync_rule_catalog(conn) -> int:
    """Upsert the registry into trade_path_rules.

    The dashboard reads column names from here rather than hardcoding them, so
    this must run whenever the registry changes — otherwise the UI would offer
    a rule whose columns do not exist, or miss one that does.
    """
    import psycopg2.extras
    rows = registry_rows()
    sql = """
    INSERT INTO trade_path_rules
        (rule_key, family, side, fill_mode, params,
         exit_bar_col, exit_return_col, is_horizon)
    VALUES %s
    ON CONFLICT (rule_key) DO UPDATE SET
        family          = EXCLUDED.family,
        side            = EXCLUDED.side,
        fill_mode       = EXCLUDED.fill_mode,
        params          = EXCLUDED.params,
        exit_bar_col    = EXCLUDED.exit_bar_col,
        exit_return_col = EXCLUDED.exit_return_col,
        is_horizon      = EXCLUDED.is_horizon
    """
    vals = [(r["rule_key"], r["family"], r["side"], r["fill_mode"],
             r["params"], r["exit_bar_col"], r["exit_return_col"],
             r["is_horizon"]) for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, vals, page_size=200)
    conn.commit()
    return len(vals)
