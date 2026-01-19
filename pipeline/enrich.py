"""
pipeline/enrich.py

Enrich focused hexes (h3_id list) with facility counts.

Input:
  - focused_hex_csv: CSV containing at least 'h3_id'
  - facilities_dir: folder with facility CSV files

Output:
  - enriched CSV: original focused hex rows + <prefix>_count columns
"""

from __future__ import annotations

from pathlib import Path
import pandas as pd
from pipeline.utils import coerce_numeric, pick_col, latlon_to_h3, get_logger
logger = get_logger(__name__)

# These match the exact files you copied into data/raw/facilities/
CATEGORIES = [
    {"name": "Transit_Stops_GTFS", "prefix": "transit"},
    {"name": "Healthcare_Facilities", "prefix": "health"},
    {"name": "Places", "prefix": "place"},
    {"name": "Colleges_SG_geocoded", "prefix": "college"},
    {"name": "Casinos_geocoded", "prefix": "casino"},
    {"name": "Elder_Care_Facilities_geocoded", "prefix": "elder"},
    {"name": "Gas_Stations_geocoded", "prefix": "gas"},
    {"name": "Library_geocoded", "prefix": "library"},
    {"name": "Prescription_Drug_Drop_Off_Sites_geocoded", "prefix": "rx_drop"},
    {"name": "Recreation_Centre_geocoded", "prefix": "rec"},
    {"name": "Recreation_Centre_geocoded2", "prefix": "rec2"},
    {"name": "Child_Care_Centers_geocoded", "prefix": "childcare"},
    {"name": "Cool_Zones_geocoded", "prefix": "coolzone"},
    {"name": "Business_Sites_geocoded", "prefix": "business"},
    {"name": "Affordable_Housing_Inventory_geocoded", "prefix": "afford_housing"},
]



def load_points_from_csv(path: Path) -> list[tuple[float, float]]:
    df = pd.read_csv(path)

    lat_candidates = ["lat", "latitude", "y", "ycoord", "y_coordinate", "point_y", "POINT_Y", "Y", "stop_lat"]
    lon_candidates = ["lon", "lng", "longitude", "x", "xcoord", "x_coordinate", "point_x", "POINT_X", "X", "stop_lon"]

    lat_col = pick_col(df, lat_candidates)
    lon_col = pick_col(df, lon_candidates)
    if lat_col is None or lon_col is None:
        raise ValueError(f"No lat/lon columns found in {path.name}")

    lat = coerce_numeric(df[lat_col])
    lon = coerce_numeric(df[lon_col])

    # swap if reversed
    if lat.abs().median() > 90 and lon.abs().median() < 90:
        lat, lon = lon, lat

    out = pd.DataFrame({"lat": lat, "lon": lon}).dropna()
    out = out[(out["lat"].between(-90, 90)) & (out["lon"].between(-180, 180))]
    return [tuple(x) for x in out[["lat", "lon"]].to_numpy()]


def run_enrich_facilities(
    focused_hex_csv: Path,
    facilities_dir: Path,
    output_csv: Path,
    hex_resolution: int,
) -> Path:
    focused_df = pd.read_csv(focused_hex_csv)
    if "h3_id" not in focused_df.columns:
        raise ValueError("focused_hex_csv must contain 'h3_id'")


    logger.info(f"Reading focused hexes: {focused_hex_csv} (rows={len(focused_df):,})")
    logger.info(f"Facilities dir: {facilities_dir}")

    result_df = focused_df.copy()

    for cat in CATEGORIES:
        name = cat["name"]
        prefix = cat["prefix"]

        facility_path = facilities_dir / f"{name}.csv"
        if not facility_path.exists():
            # since you only copied CSVs, we only support CSV in this minimal step
            logger.info(f"• {name}: missing file {facility_path.name}, setting {prefix}_count=0")
            result_df[f"{prefix}_count"] = 0
            continue

        points = load_points_from_csv(facility_path)
        if not points:
            result_df[f"{prefix}_count"] = 0
            logger.info(f"• {name}: 0 valid points")
            continue

        h_ids = [latlon_to_h3(lat, lon, hex_resolution) for (lat, lon) in points]
        df_cat = pd.DataFrame({"h3_id": h_ids})

        agg = df_cat.groupby("h3_id").size().reset_index(name="count")
        count_map = dict(zip(agg["h3_id"], agg["count"]))

        result_df[f"{prefix}_count"] = result_df["h3_id"].map(count_map).fillna(0).astype(int)
        logger.info(f"• {name}: loaded {len(points)} points")
 
    logger.info(f"Writing enriched hex CSV: {output_csv} (rows={len(result_df):,})")
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_csv, index=False)
    return output_csv

