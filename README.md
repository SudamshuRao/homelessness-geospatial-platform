# Homelessness Geospatial ETL & API Platform (San Diego)

## Overview

This project is a **geospatial data platform** that processes homelessness-related data in San Diego and exposes enriched spatial features via a **containerized FastAPI backend**.

The system ingests raw tent detection data, maps it to **H3 hexagonal grids**, enriches each hex with nearby facilities (healthcare, transit, businesses, etc.), and serves the results through REST APIs for downstream analysis, visualization, or ML workflows.

The project is designed as an **end-to-end system**, emphasizing:

- Clean data pipelines  
- Reproducibility  
- Backend service design  
- Observability  
- Testability  
- Containerized deployment  

---

## High-Level Architecture

Raw Data (CSV)
│
▼
ETL Pipeline (Python)
├─ Spatial Indexing (lat/lon → H3 hexes + k-ring expansion)
├─ Facility Enrichment (POIs → per-hex counts)
└─ Validated & Versioned Outputs
│
▼
Enriched Hex Dataset (CSV / Parquet)
│
▼
FastAPI Backend
├─ Hex lookup by ID
├─ Hex lookup by coordinates
├─ Summary / ranking endpoints
└─ Health & debug endpoints
│
▼
Docker / docker-compose


---

## Key Features

### ETL Pipeline
- Config-driven pipeline with clearly separated stages  
- Maps tent detections to H3 hex grids at configurable resolution  
- Expands hexes using k-ring neighborhoods (ring = 1 / 3 / 4)  
- Enriches each hex with counts from **15+ facility categories**, including:
  - Transit
  - Healthcare
  - Businesses
  - Childcare
  - Elder care
  - Affordable housing
  - Recreation
  - Libraries
  - Cool zones
- Structured logging for observability  
- Unit tests using **pytest**

---

## Backend API

- FastAPI-based REST service  
- Loads enriched dataset at startup for fast lookups  

### Endpoints
- `GET /health`
- `GET /hex/id/{h3_id}`
- `GET /hex/by-location`
- `GET /summary/top-tent-hexes`

### Highlights
- Robust routing (avoids path shadowing)
- Debug-friendly endpoints for validation
- JSON responses suitable for dashboards or ML services

---

## Deployment

- Fully containerized using **Docker**
- Orchestrated with **docker-compose**
- Data and configuration mounted as volumes (no hardcoded paths)
- Ready for local or cloud deployment

---

## Project Structure

homelessness-system/
├── api/ # FastAPI backend
│ └── main.py
├── pipeline/ # ETL pipeline stages
│ ├── spatial_index.py
│ ├── enrich.py
│ └── utils.py
├── scripts/ # Pipeline runner
│ └── run_pipeline.py
├── tests/ # Unit tests (pytest)
│ ├── test_spatial_index.py
│ └── test_enrich.py
├── data/
│ ├── raw/ # Input data (ignored by git)
│ └── processed/ # Pipeline outputs (ignored by git)
├── config/
│ └── config.example.yaml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md


---

## Running the ETL Pipeline Locally

```bash
python3 -m scripts.run_pipeline
This generates a versioned output folder under:

data/processed/<run_id>/

containing:
 Focused hex dataset
 Enriched hex dataset

Running the API Locally (Without Docker)
python3 -m uvicorn api.main:app --reload

Open:
http://127.0.0.1:8000/docs

Running with Docker & docker-compose (Recommended)
Build & start services
docker compose up --build
Stop services
docker compose down

Access the API
Root: http://127.0.0.1:8000/
Health: http://127.0.0.1:8000/health
Swagger UI: http://127.0.0.1:8000/docs

Example API Usage
Get hex by ID
curl http://127.0.0.1:8000/hex/id/8a29a4006907fff

Get hex by location
curl "http://127.0.0.1:8000/hex/by-location?lat=32.80325&lon=-117.21484"

Get top tent hexes
curl "http://127.0.0.1:8000/summary/top-tent-hexes?n=10"

Testing
Run all tests:
pytest

Tests cover:
Spatial indexing logic
Enrichment correctness
Edge cases on synthetic data

Engineering Highlights
Designed a modular EL pipeline with clear data contracts
Applied H3 geospatial indexing for scalable spatial analysis
Built a RESTful backend service with FastAPI
Added structured logging and unit tests
Containerized the system using Docker and docker-compose
Debugged real-world backend issues (routing conflicts, data normalization)

Future Extensions
Persist data in PostgreSQL / PostGIS
Add caching for frequent hex lookups
Integrate ML models for risk prediction
Deploy to cloud (AWS ECS / GCP Cloud Run)
Add authentication & rate limiting

Author
Sudamshu Rao
Shree Nandan Prabhakar



