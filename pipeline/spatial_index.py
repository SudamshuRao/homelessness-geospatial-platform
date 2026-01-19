"""
pipeline/spatial_index.py

Step: Generate focused H3 hexes around tent detections.
Input: raw tents CSV (lat/lon)
Output: focused hex CSV/Parquet with h3_id, center_lat, center_lon, tent_status
"""

from pathlib import Path
import pandas as pd
import h3

def latlon_to_h3(lat: float, lon: float, res: int) -> str:
    try:
        return h3.latlng_to_cell(lat, lon, res)
    except AttributeError:
        return h3.geo_to_h3(lat, lon, res)

def run_spatial_index_tents(
    tents_csv: Path,
    output_path: Path,
    hex_resolution: int,
    tent_ring: int,
) -> Path:
    # 1) load tents
    tents_df = pd.read_csv(tents_csv)

    # 2) detect lat/lon columns (we’ll improve this next step)
    lat_col = next((c for c in tents_df.columns if c.lower() in ["lat", "latitude", "y"]), None)
    lon_col = next((c for c in tents_df.columns if c.lower() in ["lon", "lng", "longitude", "x"]), None)
    if lat_col is None or lon_col is None:
        raise ValueError("Could not find lat/lon columns in tents CSV")

    tents_df = tents_df.rename(columns={lat_col: "lat", lon_col: "lon"})[["lat", "lon"]].dropna()

    # 3) map tents to h3
    tents_df["h3_id"] = [latlon_to_h3(r.lat, r.lon, hex_resolution) for r in tents_df.itertuples()]
    tent_hexes = set(tents_df["h3_id"].unique())

    # 4) expand to focused hexes using ring
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
    out_df.to_csv(output_path, index=False)
    return output_path

