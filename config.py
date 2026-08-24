"""Project configuration — loads .env once, exposes typed constants."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent

# --- Postgres ---------------------------------------------------------------
PG_HOST     = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB       = os.environ.get("POSTGRES_DB", "open_interest")
PG_USER     = os.environ.get("POSTGRES_USER", "portfolio")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "portfolio")

# --- ThetaData --------------------------------------------------------------
THETADATA_BASE_URL = os.environ.get("THETADATA_BASE_URL", "http://localhost:25503")

# --- Polygon.io / Massive ----------------------------------------------------
# Rebranded to Massive in 2026; api.polygon.io still resolves and is what the
# published REST docs use, so the host stays until the vendor retires it.
POLYGON_API_KEY  = os.environ.get("POLYGON_API_KEY", "")
POLYGON_BASE_URL = os.environ.get("POLYGON_BASE_URL", "https://api.polygon.io")

# --- Parquet store for raw OI -----------------------------------------------
_default_oi_dir = PROJECT_ROOT / "data" / "oi_raw"
OI_RAW_DIR      = Path(os.environ.get("OI_RAW_DIR", str(_default_oi_dir))).resolve()

# --- Parquet store for raw EOD greeks chain (vol + IV refactor) -------------
_default_chain_dir = PROJECT_ROOT / "data" / "chain_eod"
CHAIN_EOD_DIR      = Path(os.environ.get("CHAIN_EOD_DIR", str(_default_chain_dir))).resolve()

# --- Parquet store for twice-daily intraday chain snapshots (09:45 / 15:45) --
# Defaults to a TRUE sibling of the resolved CHAIN_EOD_DIR, not to
# PROJECT_ROOT/data.  The other stores only land in the top-level /data
# because .env overrides them (OI_RAW_DIR, CHAIN_EOD_DIR); their PROJECT_ROOT
# defaults above are never what production actually uses.  Deriving from
# CHAIN_EOD_DIR.parent means this store follows chain_eod wherever .env puts
# it and cannot drift into the project tree if the .env entry is missing.
#
# The sibling derivation is now only the FALLBACK: production sets
# CHAIN_SNAPSHOTS_DIR and CHAIN_INTRADAY_DIR explicitly, both on the dedicated
# chain volume, so neither is a sibling of chain_eod any more.  Treat the
# resolved value as the only source of truth — the fetchers print it in
# preflight — and do not infer a location from these defaults.
_default_chain_snap_dir = CHAIN_EOD_DIR.parent / "chain_snapshots"
CHAIN_SNAPSHOTS_DIR     = Path(os.environ.get("CHAIN_SNAPSHOTS_DIR",
                                              str(_default_chain_snap_dir))).resolve()

# --- Parquet store for full-day 5-minute intraday chain bars ----------------
# Same sibling-of-chain_eod derivation as CHAIN_SNAPSHOTS_DIR above, for the
# same reason: never PROJECT_ROOT, which is not where the data stores live.
_default_chain_intraday_dir = CHAIN_EOD_DIR.parent / "chain_intraday"
CHAIN_INTRADAY_DIR          = Path(os.environ.get("CHAIN_INTRADAY_DIR",
                                                  str(_default_chain_intraday_dir))).resolve()

# --- Parquet store for 1-minute equity bars (Polygon/Massive) ---------------
# Same sibling-of-chain_eod derivation as the two stores above, for the same
# reason: PROJECT_ROOT is not where bulk data lives on the VPS.
_default_equity_1min_dir = CHAIN_EOD_DIR.parent / "equity_1min"
EQUITY_1MIN_DIR          = Path(os.environ.get("EQUITY_1MIN_DIR",
                                               str(_default_equity_1min_dir))).resolve()
