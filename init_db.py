"""
init_db.py — One-time schema + view initialisation for the open_interest DB.

Run AFTER `psql -U postgres -f sql/00_create_database.sql` has created the
database itself. Re-running is safe (CREATE TABLE IF NOT EXISTS / OR REPLACE).

Usage:
    python init_db.py
"""
from __future__ import annotations

from pathlib import Path

from db import get_connection

SQL_DIR = Path(__file__).parent / "sql"
FILES   = [
    "01_schema.sql",
    "03_new_metrics.sql",
    "02_views.sql",
    "04_backtest.sql",
    "05_bin_tables.sql",
    "06_25d_skew_metrics.sql",
    "07_trade_paths.sql",
    "08_equity_surface.sql",
    "09_equity_metrics.sql",
]


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for fname in FILES:
                path = SQL_DIR / fname
                print(f"Applying {fname} ...", end=" ", flush=True)
                cur.execute(path.read_text())
                print("OK")
        conn.commit()

        # Dynamic per-metric column sync for wf_bins.  Reads
        # metric_classification.eligible_as_metric = TRUE and adds
        # frac_<metric> / bin20_<metric> column pairs idempotently.
        # Done AFTER the static SQL files so the wf_bins skeleton + the
        # metric_classification table both exist.
        try:
            from lib.bin_schema import (
                sync_is_bins_schema,
                sync_tt_bins_schema,
                sync_wf_bins_schema,
            )
            print("Syncing wf_bins per-metric columns ...", end=" ", flush=True)
            n_added, _ = sync_wf_bins_schema(conn)
            print(f"OK ({n_added} new column pair(s))")
            print("Syncing is_bins per-metric columns  ...", end=" ", flush=True)
            n_added, _ = sync_is_bins_schema(conn)
            print(f"OK ({n_added} new column pair(s))")
            print("Syncing tt_bins per-metric columns  ...", end=" ", flush=True)
            n_added, _ = sync_tt_bins_schema(conn)
            print(f"OK ({n_added} new column pair(s))")
        except Exception as e:
            print(f"SKIPPED ({type(e).__name__}: {e})")
            print("  (Run `python build_bin_tables.py --tier EVENING` later "
                  "to sync; metric_classification may not be populated yet.)")

        # Same pattern for the equity metrics tables: 09_equity_metrics.sql
        # creates key-only skeletons and the ~600 metric columns are generated
        # from lib/metrics_config.py. Without this the tables exist but hold
        # nothing, which is the failure mode the drift check exists to catch.
        try:
            from lib.metrics_store import (
                check_catalog_drift, sync_catalog, sync_metrics_schema,
            )
            print("Syncing equity_metrics columns      ...", end=" ", flush=True)
            n_base, n_z = sync_metrics_schema(conn)
            n_cat = sync_catalog(conn)
            check_catalog_drift(conn)
            print(f"OK ({n_base} base + {n_z} z column(s), "
                  f"{n_cat} catalog row(s))")
        except Exception as e:
            print(f"FAILED ({type(e).__name__}: {e})")
            print("  (Run `python build_equity_metrics.py init-db` to retry.)")
    print("\nDatabase initialised.")


if __name__ == "__main__":
    main()
