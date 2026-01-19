from pathlib import Path

import pandas as pd
import yaml
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from pipeline.utils import latlon_to_h3

from pipeline.utils import latlon_to_h3


class HexRecord(BaseModel):
    h3_id: str
    center_lat: float
    center_lon: float
    tent_status: int
    # We don't list every facility column here; we’ll return a generic dict for them.


app = FastAPI(title="Homelessness Hex Enrichment API", version="0.1.0")


def load_config(config_path: str = "config/config.example.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def load_enriched_df(enriched_csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(enriched_csv_path)
    if "h3_id" not in df.columns:
        raise ValueError("Enriched CSV must contain 'h3_id' column")
    df["h3_id"] = df["h3_id"].astype(str).str.strip()   
    return df.set_index("h3_id")


@app.get("/")
def root():
    return {
        "service": "Homelessness Hex Enrichment API",
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
    }





@app.on_event("startup")
def startup_event():
    cfg = load_config()
    enriched_path = Path(cfg["service"]["enriched_csv_path"])

    if not enriched_path.exists():
        raise RuntimeError(f"Enriched CSV not found at {enriched_path}")

    app.state.enriched_df = load_enriched_df(enriched_path)


@app.get("/debug/lookup")
def debug_lookup(h3_id: str):
    df: pd.DataFrame = app.state.enriched_df
    return {
        "h3_id": h3_id,
        "in_index": h3_id in df.index,
        "index_dtype": str(df.index.dtype),
        "index_name": df.index.name,
        "columns_sample": list(df.columns[:10]),
        "rows": int(df.shape[0]),
    }



@app.get("/health")
def health():
    df = getattr(app.state, "enriched_df", None)
    return {
        "status": "ok" if df is not None else "not_ready",
        "rows": int(df.shape[0]) if df is not None else 0,
    }


@app.get("/hex/by-location")
def get_hex_by_location(
    lat: float = Query(..., description="Latitude in degrees"),
    lon: float = Query(..., description="Longitude in degrees"),
    resolution: int = Query(10, description="H3 resolution"),
):
    """
    Debug-friendly version:
    - Always returns JSON (never raises 404)
    - Shows computed_h3_id and whether it's in the DataFrame index
    """
    df: pd.DataFrame = app.state.enriched_df

    h3_id = latlon_to_h3(lat, lon, resolution)
    in_index = h3_id in df.index

    # Base debug info
    result = {
        "lat": lat,
        "lon": lon,
        "resolution": resolution,
        "computed_h3_id": h3_id,
        "in_index": in_index,
    }

    # If found, include the row
    if in_index:
        result["row"] = df.loc[h3_id].to_dict()

    return result


@app.get("/hex/{h3_id}")
def get_hex_by_id(h3_id: str):
    df: pd.DataFrame = app.state.enriched_df
    if h3_id not in df.index:
        raise HTTPException(status_code=404, detail="Hex not found")

    row = df.loc[h3_id]

    # Split core columns vs facilities
    core = {
        "h3_id": h3_id,
        "center_lat": float(row["center_lat"]),
        "center_lon": float(row["center_lon"]),
        "tent_status": int(row["tent_status"]),
    }

    facilities = {
        col: int(row[col])
        for col in df.columns
        if col.endswith("_count")
    }

    return {
        "core": core,
        "facilities": facilities,
    }




