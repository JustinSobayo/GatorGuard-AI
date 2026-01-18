# GatorGuard-AI

**Real-time Predictive Crime Analysis & Safety Platform**

GainesvilleGuard-AI is an advanced data engineering and AI platform designed to analyze crime data in Gainesville, FL, in real-time. By leveraging industry-grade streaming pipelines and graph-based machine learning, it provides low-latency predictive risk assessments to enhance community safety.

## Project Goal
To move beyond simple historical crime mapping by building a system that can **ingest live data**, **process complex relationships** (via Knowledge Graphs), and **forecast crime risk** for specific times and locations.

## System Architecture
The system follows a modern Event-Driven Architecture (EDA):

*   **Ingestion**: Python Producers stream crime data into **Apache Kafka**.
*   **Processing**: **Apache Spark Structured Streaming** processes raw events in real-time.
*   **Knowledge Graph**: **Neo4j** models spatial-temporal relationships (crime clusters, environmental risk factors, location-based patterns).
*   **Storage**: 
    *   **PostgreSQL + PostGIS** for geospatial fact storage and fast map querying.
    *   **AWS S3** for data lake archival.
*   **Frontend**: A responsive web application visualizing "Past, Current/Actual, and Predicted" crime heatmaps.

## Technology Stack
This project showcases a "Heavy" Data Engineering stack designed for scale:

*   **Backend**: Python, FastAPI
*   **Streaming**: Apache Kafka, Zookeeper
*   **ETL & Processing**: Apache Spark
*   **Databases**: 
    *   **PostgreSQL** (Relational)
    *   **PostGIS** (Geospatial Optimization)
    *   **Neo4j** (Graph Database)
*   **Infrastructure**: Docker, Docker Compose

## Why Geospatial Optimization (PostGIS)?
We use **PostGIS** to handle the heavy lifting of spatial queries. 
*   **Performance**: Since the map covers a large area, we need to efficiently query only the data visible on the screen. PostGIS uses **R-Tree indices** to instantly fetch points within a specific "Bounding Box" (the current map view) or Grid Cell.
*   **Spatial Aggregation**: Essential for the predictive model, which divides Gainesville into thousands of fixed grid cells. PostGIS allows us to "Count crimes inside Grid X" instantly, which is critical for training the AI model and generating heatmaps.

## Predictive Crime Prevention
This system utilizes **Spatial-Temporal Pattern Analysis** methodologies:

### 1. Risk Terrain Modeling (RTM)
Identifies environmental factors that contribute to crime risk by connecting crime events to nearby points of interest (bars, ATMs, bus stops) from OpenStreetMap data.

### 2. Graph-Based Pattern Detection
Uses Neo4j to model relationships:
*   **Crime Clusters**: `(Crime)-[:NEAR]->(Crime)` - Crimes within 500m and 7 days
*   **Environmental Context**: `(Crime)-[:OCCURRED_NEAR]->(Place)` - Proximity to high-risk locations
*   **Temporal Patterns**: `(Crime)-[:DURING]->(TimeWindow)` - Time-of-day and day-of-week clustering
*   **Grid Risk Scores**: `(Grid)-[:HIGH_RISK_FOR]->(CrimeType)` - Predictive risk by location and crime type

### 3. LLM-Powered Explanations
When users click a high-risk zone, an LLM (Gemini/Groq) queries the Knowledge Graph to generate natural language explanations:
> "This area shows elevated risk due to 12 thefts within 200 meters in the last 30 days, with 8 occurring near University Ave ATMs between 10 PM - 2 AM, matching historical Friday night patterns."

## License
This project is proprietary software. All rights reserved.
See `LICENSE` file for details.
