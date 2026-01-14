"""
POI (Points of Interest) Data Processor (Spark Structured Streaming)

Consumes POI data from Kafka, transforms it, and writes to:
- PostgreSQL/PostGIS for geospatial analysis and joining with crime data

Data Flow: Kafka (poi_data topic) --> Spark --> Postgres

Note: POI data is used for Risk Terrain Modeling - correlating crime 
locations with nearby amenities (bars, ATMs, schools, etc.)
"""

import os

from dotenv import load_dotenv

# TODO: Uncomment after confirming PySpark is installed
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'poi_data'

# PostgreSQL connection
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gatorguard')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')


def get_spark_session():
    """
    TODO: Create and return a SparkSession with necessary packages.
    
    Steps:
    1. Use SparkSession.builder
    2. Set appName to "POIDataProcessor"
    3. Add packages for Kafka and PostgreSQL JDBC:
       - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
       - org.postgresql:postgresql:42.6.0
    
    Example:
        return SparkSession.builder \\
            .appName("POIDataProcessor") \\
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.6.0") \\
            .getOrCreate()
    """
    pass


def get_poi_schema():
    """
    TODO: Define the schema for POI data from Kafka.
    
    The POI JSON from OpenStreetMap (via ingest_osm.py) has fields:
    - id: Long (OSM element ID)
    - type: String ("node" or "way")
    - amenity: String (bar, restaurant, atm, bank, school, university, parking, etc.)
    - name: String (name of the place)
    - lat: Double (latitude)
    - lon: Double (longitude)
    
    Return a StructType matching this schema.
    """
    pass


def read_from_kafka(spark):
    """
    TODO: Create a streaming DataFrame reading from Kafka.
    
    Steps:
    1. Use spark.readStream.format("kafka")
    2. Set option "kafka.bootstrap.servers" to KAFKA_BROKER
    3. Set option "subscribe" to TOPIC_NAME
    4. Set option "startingOffsets" to "earliest"
    5. Load and return the DataFrame
    """
    pass


def transform_poi_data(df, schema):
    """
    TODO: Transform raw Kafka data into structured POI records.
    
    Steps:
    1. Cast 'value' column from bytes to string
    2. Use from_json() to parse JSON string with the schema
    3. Select and rename columns to match your target table:
       - poi_id (from id)
       - osm_type (from type)
       - amenity_type (from amenity)
       - name
       - latitude (from lat)
       - longitude (from lon)
    4. Optionally add a geometry column for PostGIS:
       - ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
       (This might be done in SQL after loading)
    
    Return the transformed DataFrame.
    """
    pass


def write_to_postgres(df, table_name: str):
    """
    TODO: Write DataFrame to PostgreSQL using JDBC.
    
    For streaming, use foreachBatch:
        def write_batch(batch_df, batch_id):
            batch_df.write \\
                .format("jdbc") \\
                .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}") \\
                .option("dbtable", table_name) \\
                .option("user", POSTGRES_USER) \\
                .option("password", POSTGRES_PASSWORD) \\
                .option("driver", "org.postgresql.Driver") \\
                .mode("append") \\
                .save()
        
        return df.writeStream.foreachBatch(write_batch)
    
    Note: The target table should exist in Postgres (see init_db.sql).
    """
    pass


def process_stream():
    """
    TODO: Main streaming pipeline.
    
    Steps:
    1. Get SparkSession
    2. Define schema
    3. Read from Kafka
    4. Transform data
    5. Write to Postgres (streaming)
    6. Await termination
    
    Example structure:
        spark = get_spark_session()
        schema = get_poi_schema()
        
        raw_df = read_from_kafka(spark)
        transformed_df = transform_poi_data(raw_df, schema)
        
        postgres_query = write_to_postgres(transformed_df, "points_of_interest") \\
            .start()
        
        postgres_query.awaitTermination()
    """
    print("POI Processor starting...")
    print(f"Reading from Kafka: {KAFKA_BROKER}/{TOPIC_NAME}")
    print(f"Writing to Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    # TODO: Implement the streaming pipeline
    
    print("POI Processor skeleton - implement the TODO sections")


if __name__ == "__main__":
    process_stream()

