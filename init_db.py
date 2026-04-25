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
FILES   = ["01_schema.sql", "02_views.sql"]


def main() -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            for fname in FILES:
                path = SQL_DIR / fname
                print(f"Applying {fname} ...", end=" ", flush=True)
                cur.execute(path.read_text())
                print("OK")
        conn.commit()
    print("\nDatabase initialised.")


if __name__ == "__main__":
    main()
