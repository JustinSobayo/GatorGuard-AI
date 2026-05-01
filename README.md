# GatorGuard-AI

**Predictive geospatial safety advice for Gainesville, FL**

GatorGuard-AI is a data engineering and AI project that turns Gainesville crime records and OpenStreetMap location data into a local safety map. The system loads raw data into a data lake, processes it through an ETL pipeline, stores spatial and graph context in databases, and serves predicted risk zones with plain-English safety advice.

## What It Does

- Ingests historical Gainesville crime data and OpenStreetMap points of interest.
- Stores raw source data in AWS S3.
- Streams and cleans crime records with Kafka and Spark.
- Loads geospatial crime facts into PostgreSQL/PostGIS.
- Loads crime, time, location, and nearby-place relationships into Neo4j.
- Generates daily grid-cell risk predictions for the map.
- Serves prediction heatmaps and AI-generated safety advice through FastAPI.
- Displays predicted high-risk zones in a Leaflet frontend.

<img width="1893" height="882" alt="Screenshot 2026-04-30 224131" src="https://github.com/user-attachments/assets/32c4bdc7-b5cf-4eac-a076-71f080a64615" />


## Architecture

```text
Gainesville API + OpenStreetMap
-> AWS S3 raw data lake
-> Kafka producer
-> Spark Structured Streaming ETL
-> PostgreSQL/PostGIS for spatial facts and predictions
-> Neo4j for relationship context
-> FastAPI backend
-> Leaflet frontend
-> Gemini/LangChain explanation layer
```

The key design choice is separating expensive processing from real-time serving:

- **ETL pipeline:** loads and cleans crime/POI data.
- **PostGIS:** stores map geometry, crime points, grid cells, and cached prediction rows.
- **Neo4j:** stores relationship context used to explain why an area may be risky.
- **FastAPI:** exposes history, prediction, and advice endpoints.
- **Gemini via LangChain:** turns structured facts into user-facing safety advice.

## MVP Features

- Predicted heatmap cells from `daily_grid_predictions`.
- Click-to-advice flow for selected risk zones.
- Structured advice response with:
  - risk level
  - risk score
  - dominant crime type
  - explanation
  - safety advice
  - supporting facts
- Local frontend for testing the full backend flow.

## Technology Stack

- **Backend:** Python, FastAPI
- **Frontend:** HTML, CSS, JavaScript, Leaflet
- **Streaming:** Apache Kafka, Zookeeper
- **ETL:** Apache Spark Structured Streaming
- **Storage:** AWS S3
- **Databases:** PostgreSQL, PostGIS, Neo4j
- **AI:** LangChain, Gemini
- **Infrastructure:** Docker, Docker Compose

## Predictive Safety Method

The project combines spatial, temporal, and relationship-based signals. PostGIS counts crime events inside fixed Gainesville grid cells, while Neo4j connects incidents to nearby places and time patterns. A prediction job precomputes risk scores so the frontend can load heatmap cells quickly without running expensive spatial aggregation during user requests.

<img width="1315" height="514" alt="Screenshot 2026-01-29 184714" src="https://github.com/user-attachments/assets/34ed9cf9-a7d0-481d-a1ca-678c3c3ba81a" />

The image illustrates the project idea: instead of only showing where crimes happened in the past, GatorGuard-AI combines historical incidents, location context, and time patterns to identify areas with elevated future risk.

### Risk Terrain Modeling

OpenStreetMap points of interest, such as parking areas, ATMs, restaurants, schools, and nightlife locations, provide environmental context around incidents.

### Spatial-Temporal Analysis

PostGIS stores crime points and grid polygons, then calculates crime counts by location, day, and recent activity. These counts feed the daily prediction job.

### Graph-Based Context

Neo4j models relationships such as:

- incidents occurring at locations
- incidents occurring during time blocks
- incidents near places of interest
- seasonal and day-of-week patterns

### LLM-Powered Explanations

When a user clicks a predicted risk zone, the backend combines PostGIS prediction metadata with Neo4j explanation facts. Gemini then generates calm, practical safety advice using only the provided data.

Example:

> "This area shows elevated risk during the next 24 hours based on historical incidents, recent activity, and nearby location context. Prefer well-lit routes and stay aware around parking areas."

## Local MVP Testing

Backend:

```powershell
.\venv\Scripts\python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8000
```

Frontend:

```powershell
cd frontend
..\venv\Scripts\python.exe -m http.server 5500 --bind 127.0.0.1
```

Open:

```text
http://127.0.0.1:5500
```

Useful API endpoints:

- `GET /crimes/predict?date=today&min_risk_level=medium`
- `GET /predict/advice?grid_id=<grid_id>`
- `GET /crimes/history?date=YYYY-MM-DD&limit=1000`

## License

This project is proprietary software. All rights reserved.
See `LICENSE` for details.
