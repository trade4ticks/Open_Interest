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

# --- OI surface filtering (legacy — surface table is no longer populated) ---
OI_MIN           = int(os.environ.get("OI_MIN", "100"))
OI_MAX_DTE       = int(os.environ.get("OI_MAX_DTE", "365"))
OI_MAX_MONEYNESS = float(os.environ.get("OI_MAX_MONEYNESS", "0.50"))
