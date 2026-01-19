from pathlib import Path

import pandas as pd

from pipeline.utils import latlon_to_h3
from pipeline.enrich import run_enrich_facilities


def test_enrich_adds_facility_counts(tmp_path):
    # Use a synthetic lat/lon and H3 id
    lat, lon = 32.70, -117.16
    res = 10
    h3_id = latlon_to_h3(lat, lon, res)

    # Focused hexes CSV with a single hex
    focused_csv = tmp_path / "focused_hexes.csv"
    pd.DataFrame(
        {
            "h3_id": [h3_id],
            "center_lat": [lat],
            "center_lon": [lon],
            "tent_status": [1],
        }
    ).to_csv(focused_csv, index=False)

    # Facilities directory with a single category file: Transit_Stops_GTFS.csv
    facilities_dir = tmp_path / "facilities"
    facilities_dir.mkdir()

    # Two facility points in the same hex → expect count=2
    pd.DataFrame(
        {
            "lat": [lat, lat],
            "lon": [lon, lon],
        }
    ).to_csv(facilities_dir / "Transit_Stops_GTFS.csv", index=False)

    enriched_csv = tmp_path / "enriched.csv"

    out_path = run_enrich_facilities(
        focused_hex_csv=focused_csv,
        facilities_dir=facilities_dir,
        output_csv=enriched_csv,
        hex_resolution=res,
    )

    assert out_path.exists(), "Enriched CSV should be created"

    df = pd.read_csv(out_path)

    # transit_count should be present and equal to 2
    assert "transit_count" in df.columns
    assert df.loc[0, "transit_count"] == 2

    # Other categories either won't exist or should be zero if present
    for col in df.columns:
        if col.endswith("_count") and col != "transit_count":
            assert df[col].iloc[0] == 0

