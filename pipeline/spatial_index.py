"""
pipeline/spatial_index.py

Step: Generate focused H3 hexes around tent detections.
Input: raw tents CSV (lat/lon)
Output: focused hex CSV/Parquet with h3_id, center_lat, center_lon, tent_status
"""

from pathlib import Path
import pandas as pd
import h3
from pipeline.utils import latlon_to_h3, detect_lat_lon_columns
from pipeline.utils import latlon_to_h3, detect_lat_lon_columns, get_logger
logger = get_logger(__name__)



def run_spatial_index_tents(
    tents_csv: Path,
    output_path: Path,
    hex_resolution: int,
    tent_ring: int,
) -> Path:
    # 1) load tents
    tents_df = pd.read_csv(tents_csv)
    logger.info(f"Loaded {len(tents_df):,} tent points")


    # 2) detect lat/lon columns (we’ll improve this next step)
    lat_col, lon_col = detect_lat_lon_columns(tents_df)
    logger.info(f"Detected lat/lon columns: {lat_col}, {lon_col}")


    tents_df = tents_df.rename(columns={lat_col: "lat", lon_col: "lon"})[["lat", "lon"]].dropna()
    logger.info(f"Valid tent points after dropna: {len(tents_df):,}")

    # 3) map tents to h3
    tents_df["h3_id"] = [latlon_to_h3(r.lat, r.lon, hex_resolution) for r in tents_df.itertuples()]
    tent_hexes = set(tents_df["h3_id"].unique())
    logger.info(f"Tents span {len(tent_hexes):,} unique hexes at res={hex_resolution}")
	

    # 4) expand to focused hexes using ring
    logger.info(f"Expanding hexes with ring={tent_ring}")
    focused_hexes = set()
    for hx in tent_hexes:
        try:
            ring_hexes = h3.grid_disk(hx, tent_ring)
        except Exception:
            try:
                ring_hexes = h3.k_ring(hx, tent_ring)
            except Exception:
                ring_hexes = {hx}
        focused_hexes.update(ring_hexes)

    logger.info(f"Expanded to {len(focused_hexes):,} focused hexes")

    # 5) build output
    rows = []
    for hx in focused_hexes:
        c_lat, c_lon = h3.cell_to_latlng(hx)
        rows.append(
            {
                "h3_id": hx,
                "center_lat": c_lat,
                "center_lon": c_lon,
                "tent_status": 1 if hx in tent_hexes else 0,
            }
        )

    out_df = pd.DataFrame(rows).sort_values("h3_id").reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    
    logger.info(f"Writing focused hex CSV: {output_path} (rows={len(out_df):,})")
    out_df.to_csv(output_path, index=False)
    return output_path

