from __future__ import annotations

from typing import Optional
import pandas as pd
import h3


def coerce_numeric(s: pd.Series) -> pd.Series:
    cleaned = (
        s.astype(str)
        .str.strip()
        .str.replace(r"[^\d\.\-\+eE]", "", regex=True)
    )
    return pd.to_numeric(cleaned, errors="coerce")


def pick_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def latlon_to_h3(lat: float, lon: float, res: int) -> str:
    try:
        return h3.latlng_to_cell(lat, lon, res)
    except AttributeError:
        return h3.geo_to_h3(lat, lon, res)


def detect_lat_lon_columns(df: pd.DataFrame) -> tuple[str, str]:
    """
    Detect latitude/longitude columns using common naming conventions.
    Returns (lat_col, lon_col) with original column names.
    """
    lat_col = next((c for c in df.columns if c.lower() in ["lat", "latitude", "y"]), None)
    lon_col = next((c for c in df.columns if c.lower() in ["lon", "lng", "longitude", "x"]), None)
    if lat_col is None or lon_col is None:
        raise ValueError(f"Could not detect lat/lon columns. Columns found: {list(df.columns)}")
    return lat_col, lon_col

import logging


def get_logger(name: str) -> logging.Logger:
    """
    Standard logger for the pipeline. Uses INFO by default.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

