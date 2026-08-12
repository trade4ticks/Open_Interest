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
_default_chain_snap_dir = CHAIN_EOD_DIR.parent / "chain_snapshots"
CHAIN_SNAPSHOTS_DIR     = Path(os.environ.get("CHAIN_SNAPSHOTS_DIR",
                                              str(_default_chain_snap_dir))).resolve()
