"""
Crime Data Processor (Spark Structured Streaming)

Consumes crime data from Kafka, transforms it, and writes to:
- PostgreSQL/PostGIS for geospatial analysis
- Neo4j for knowledge graph / Graph RAG
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    date_format,
    from_json,
    hour,
    lit,
    to_timestamp,
    when,
)
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_DIR = os.path.dirname(BACKEND_DIR)
for path in (PROJECT_DIR, BACKEND_DIR):
    if path not in sys.path:
        sys.path.insert(0, path)

try:
    from backend.postgis_ops import ensure_prediction_schema
except ImportError:  # pragma: no cover - supports direct container execution
    from postgis_ops import ensure_prediction_schema

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC_NAME = 'raw_crime_data'

# PostgreSQL connection
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gainesville_crime')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'admin')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password')

# Neo4j connection
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://neo4j:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')


def get_spark_session():
    try:
        return SparkSession.builder \
            .appName("CrimeDataProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        raise e

def get_crime_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("incident_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("report_date", StringType(), True),
        StructField("offense_date", StringType(), True),
        StructField("offense_hour_of_day", StringType(), True),
        StructField("offense_day_of_week", StringType(), True),
        StructField("address", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
    ])


def write_batch_to_postgres(batch_df, batch_id):
    """Callback for Postgres ingestion."""
    rows = batch_df.collect()
    if not rows: return
    
    print(f"Batch {batch_id}: Writing {len(rows)} records to Postgres...")
    
    data = []
    for r in rows:
        if not r["id"] or r["latitude"] is None or r["longitude"] is None:
            continue

        incident_type = r["incident_type"] or "unknown"
        description = r["description"] or incident_type
        data.append(
            (
                r["id"], incident_type, description,
                r["offense_date"], r["report_date"],
                r["latitude"], r["longitude"], r["address"],
                r["city"], r["state"], r["offense_hour"],
                r["offense_day_of_week"], r["geometry"],
            )
        )

    if not data:
        print(f"Batch {batch_id}: No valid geocoded rows to write to Postgres")
        return
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        query = """
        INSERT INTO historical_crimes (
            id, incident_type, description, 
            offense_date, report_date, 
            latitude, longitude, address, 
            city, state, offense_hour, 
            offense_day_of_week, geometry
        ) VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            incident_type = EXCLUDED.incident_type,
            description = EXCLUDED.description,
            offense_date = EXCLUDED.offense_date,
            report_date = EXCLUDED.report_date,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            offense_hour = EXCLUDED.offense_hour,
            offense_day_of_week = EXCLUDED.offense_day_of_week,
            geometry = EXCLUDED.geometry,
            updated_at = NOW()
        """
        template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326))"
        execute_values(cur, query, data, template=template)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Batch {batch_id}: Successfully committed to Postgres")
    except Exception as e:
        print(f"Error writing batch {batch_id} to Postgres: {e}")

def write_batch_to_neo4j(batch_df, batch_id):
    """Callback for Neo4j ingestion."""
    from neo4j import GraphDatabase
    
    rows = batch_df.collect()
    if not rows: return
    
    print(f"Batch {batch_id}: Writing {len(rows)} records to Neo4j...")
    
    def get_season(month):
        if month in [12, 1, 2]: return "Winter"
        elif month in [3, 4, 5]: return "Spring"
        elif month in [6, 7, 8]: return "Summer"
        else: return "Fall"
    
    def get_day_part(hour):
        if 5 <= hour < 12: return "morning"
        elif 12 <= hour < 17: return "afternoon"
        elif 17 <= hour < 21: return "evening"
        else: return "night"

    def ensure_neo4j_schema(session):
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Incident) REQUIRE i.case_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (l:Location) REQUIRE l.location_id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.lat)",
            "CREATE INDEX IF NOT EXISTS FOR (l:Location) ON (l.lon)",
            "CREATE INDEX IF NOT EXISTS FOR (t:TimeBlock) ON (t.hour)",
            "CREATE INDEX IF NOT EXISTS FOR (d:Day) ON (d.name)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Season) ON (s.name)",
        ]
        for query in queries:
            session.run(query)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            ensure_neo4j_schema(session)
            graph_rows = []
            for row in rows:
                case_id = row['id']
                if not case_id: continue
                
                incident_type = row['incident_type'] or "unknown"
                address = row['address'] or "Unknown location"
                lat = row['latitude']
                lon = row['longitude']
                hour = row['offense_hour']
                day_name = row['offense_day_of_week']
                
                month = 1
                if row['offense_date']:
                    month = row['offense_date'].month
                
                season = get_season(month)
                day_part = get_day_part(hour if hour is not None else 0)
                
                if lat is None or lon is None:
                    continue

                location_id = f"{float(lat):.6f},{float(lon):.6f}"

                graph_rows.append(
                    {
                        "location_id": location_id,
                        "address": address,
                        "lat": float(lat),
                        "lon": float(lon),
                        "case_id": case_id,
                        "type": incident_type,
                        "description": row["description"] or incident_type,
                        "hour": hour if hour is not None else 0,
                        "day_part": day_part,
                        "day_name": day_name,
                        "season": season,
                    }
                )

            if not graph_rows:
                print(f"Batch {batch_id}: No valid geocoded rows to write to Neo4j")
                return

            query = """
            UNWIND $rows AS row
            MERGE (l:Location {location_id: row.location_id})
            SET l.address = row.address,
                l.lat = row.lat,
                l.lon = row.lon,
                l.type = 'street'

            MERGE (i:Incident {case_id: row.case_id})
            SET i.type = row.type,
                i.description = row.description

            MERGE (tb:TimeBlock {hour: row.hour})
            SET tb.part_of_day = row.day_part

            MERGE (d:Day {name: row.day_name})
            MERGE (s:Season {name: row.season})

            MERGE (i)-[:OCCURRED_AT]->(l)
            MERGE (i)-[:OCCURRED_DURING]->(tb)
            MERGE (i)-[:ON_DAY]->(d)
            MERGE (i)-[:DURING_SEASON]->(s)
            """
            session.run(query, rows=graph_rows)
            print(f"Batch {batch_id}: Successfully committed to Neo4j")
    except Exception as e:
        print(f"Error writing batch {batch_id} to Neo4j: {e}")
    finally:
        driver.close()

def process_stream():
    print("Crime Processor starting...")
    print("Ensuring PostGIS schema exists...")
    ensure_prediction_schema()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    schema = get_crime_schema()
    
    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()
    
    transformed_df = raw_df.withColumn("json", from_json(col("value").cast("string"), schema)) \
        .select("json.*") \
        .withColumn("offense_date", to_timestamp(col("offense_date"))) \
        .withColumn("report_date", to_timestamp(col("report_date"))) \
        .withColumn("latitude", col("latitude").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double")) \
        .withColumn("offense_hour", coalesce(col("offense_hour_of_day").cast("integer"), hour(col("offense_date")))) \
        .withColumn("offense_day_of_week", coalesce(col("offense_day_of_week"), date_format(col("offense_date"), "EEEE"))) \
        .withColumn(
            "geometry",
            when(
                col("longitude").isNotNull() & col("latitude").isNotNull(),
                concat(lit("POINT("), col("longitude"), lit(" "), col("latitude"), lit(")")),
            ),
        ) \
        .filter(col("id").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())
    
    # Start both streams
    print("Starting dual-sink streams...")
    posgres_query = transformed_df.writeStream.foreachBatch(write_batch_to_postgres).start()
    neo4j_query = transformed_df.writeStream.foreachBatch(write_batch_to_neo4j).start()
    
    print("Streams active. Monitoring batches...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
