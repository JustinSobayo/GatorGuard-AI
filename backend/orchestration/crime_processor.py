"""
Crime Data Processor (Spark Structured Streaming)

Consumes crime data from Kafka, transforms it, and writes to:
- PostgreSQL/PostGIS for geospatial analysis
- Neo4j for knowledge graph / Graph RAG
"""

import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

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
        StructField("narrative", StringType(), True),
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
    
    data = [
        (
            r['id'], r['incident_type'], r['incident_type'],
            r['offense_date'], r['report_date'],
            r['latitude'], r['longitude'], r['address'],
            r['city'], r['state'], r['offense_hour'],
            r['offense_day_of_week'], r['geometry']
        ) for r in rows
    ]
    
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
        ON CONFLICT (id) DO NOTHING
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

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    try:
        with driver.session() as session:
            for row in rows:
                case_id = row['id']
                if not case_id: continue
                
                incident_type = row['incident_type']
                address = row['address']
                lat = row['latitude']
                lon = row['longitude']
                hour = row['offense_hour']
                day_name = row['offense_day_of_week']
                
                month = 1
                if row['offense_date']:
                    month = row['offense_date'].month
                
                season = get_season(month)
                day_part = get_day_part(hour if hour is not None else 0)
                
                query = """
                MERGE (l:Location {address: $address})
                ON CREATE SET l.lat = $lat, l.lon = $lon, l.type = 'street'

                MERGE (i:Incident {case_id: $case_id})
                SET i.type = $type, i.description = $type

                MERGE (tb:TimeBlock {hour: $hour})
                SET tb.part_of_day = $day_part

                MERGE (d:Day {name: $day_name})
                MERGE (s:Season {name: $season})

                MERGE (i)-[:OCCURRED_AT]->(l)
                MERGE (i)-[:OCCURRED_DURING]->(tb)
                MERGE (i)-[:ON_DAY]->(d)
                MERGE (i)-[:DURING_SEASON]->(s)
                """
                session.run(query,
                            address=address, lat=lat, lon=lon,
                            case_id=case_id, type=incident_type,
                            hour=hour if hour is not None else 0, day_part=day_part,
                            day_name=day_name, season=season)
            print(f"Batch {batch_id}: Successfully committed to Neo4j")
    except Exception as e:
        print(f"Error writing batch {batch_id} to Neo4j: {e}")
    finally:
        driver.close()

def process_stream():
    print("Crime Processor starting...")
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
        .withColumnRenamed("narrative", "incident_type") \
        .withColumn("offense_date", to_timestamp(col("offense_date"))) \
        .withColumn("report_date", to_timestamp(col("report_date"))) \
        .withColumn("latitude", col("latitude").cast("double")) \
        .withColumn("longitude", col("longitude").cast("double")) \
        .withColumn("offense_hour", col("offense_hour_of_day").cast("integer")) \
        .withColumn("geometry", concat(lit("POINT("), col("longitude"), lit(" "), col("latitude"), lit(")")))
    
    # Start both streams
    print("Starting dual-sink streams...")
    posgres_query = transformed_df.writeStream.foreachBatch(write_batch_to_postgres).start()
    neo4j_query = transformed_df.writeStream.foreachBatch(write_batch_to_neo4j).start()
    
    print("Streams active. Monitoring batches...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    process_stream()
