from pathlib import Path

import pandas as pd

from pipeline.spatial_index import run_spatial_index_tents


def test_spatial_index_generates_focused_hexes(tmp_path):
    # Create a small synthetic tents CSV
    tents = pd.DataFrame(
        {
            "lat": [32.70, 32.7005],
            "lon": [-117.16, -117.1605],
        }
    )
    tents_csv = tmp_path / "Tents.csv"
    tents.to_csv(tents_csv, index=False)

    # Where to write the focused hexes output
    output_csv = tmp_path / "focused_hexes.csv"

    out_path = run_spatial_index_tents(
        tents_csv=tents_csv,
        output_path=output_csv,
        hex_resolution=10,
        tent_ring=1,
    )

    # Basic checks
    assert out_path.exists(), "Focused hex CSV should be created"

    df = pd.read_csv(out_path)

    # Required columns
    for col in ["h3_id", "center_lat", "center_lon", "tent_status"]:
        assert col in df.columns

    # There should be at least one hex that actually contains a tent
    assert (df["tent_status"] == 1).any()

    # And tent_status should only be 0 or 1
    assert set(df["tent_status"].unique()).issubset({0, 1})

