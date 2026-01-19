from pathlib import Path
import yaml
from datetime import datetime

from pipeline.spatial_index import run_spatial_index_tents
from pipeline.enrich import run_enrich_facilities


def main(config_path: str):
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    tents_csv = Path(cfg["data"]["tents_csv"])
    facilities_dir = Path(cfg["data"]["facilities_dir"])
    processed_dir = Path(cfg["data"]["processed_dir"])

    h3_res = int(cfg["pipeline"]["h3_resolution"])
    tent_ring = int(cfg["pipeline"]["tent_ring"])

    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = processed_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # ---- Spatial indexing ----
    focused_hex_csv = run_dir / f"focused_hexes_ring{tent_ring}_res{h3_res}.csv"

    out_path = run_spatial_index_tents(
        tents_csv=tents_csv,
        output_path=focused_hex_csv,
        hex_resolution=h3_res,
        tent_ring=tent_ring,
    )

    print(f"Spatial indexing complete: {out_path}")

    # ---- Enrichment ----
    enriched_csv = run_dir / f"focused_hexes_enriched_ring{tent_ring}_res{h3_res}.csv"

    out_enriched = run_enrich_facilities(
        focused_hex_csv=out_path,
        facilities_dir=facilities_dir,
        output_csv=enriched_csv,
        hex_resolution=h3_res,
    )

    print(f"Enrichment complete: {out_enriched}")


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/config.example.yaml"
    main(config_path)
