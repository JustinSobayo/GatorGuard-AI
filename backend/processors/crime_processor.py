"""
Crime Data Processor (Spark Structured Streaming)

Consumes crime data from Kafka, transforms it, and writes to:
- PostgreSQL/PostGIS for geospatial analysis
- Neo4j for knowledge graph / Graph RAG

Data Flow: Kafka (raw_crime_data topic) --> Spark --> Postgres + Neo4j
"""

import os

from dotenv import load_dotenv

# TODO: Uncomment after confirming PySpark is installed
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'

# PostgreSQL connection
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'gatorguard')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')

# Neo4j connection
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')


def get_spark_session():
    """
    TODO: Create and return a SparkSession with necessary packages.
    
    Steps:
    1. Use SparkSession.builder
    2. Set appName to "CrimeDataProcessor"
    3. Add packages for Kafka and PostgreSQL JDBC:
       - org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
       - org.postgresql:postgresql:42.6.0
    
    Example:
        return SparkSession.builder \\
            .appName("CrimeDataProcessor") \\
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                    "org.postgresql:postgresql:42.6.0") \\
            .getOrCreate()
    """
    pass


def get_crime_schema():
    """
    TODO: Define the schema for crime data from Kafka.
    
    The raw JSON from Gainesville API has fields like:
    - id: String (incident ID)
    - narrative: String (crime type/description)
    - report_date: String (timestamp)
    - offense_date: String (timestamp)
    - address: String
    - latitude: String (needs casting to Double)
    - longitude: String (needs casting to Double)
    - city, state: String
    
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
    4. Set option "startingOffsets" to "earliest" (for batch) or "latest" (for real-time)
    5. Load and return the DataFrame
    
    The returned DataFrame will have columns: key, value, topic, partition, offset, timestamp
    The 'value' column contains your JSON as bytes.
    """
    pass


def transform_crime_data(df, schema):
    """
    TODO: Transform raw Kafka data into structured crime records.
    
    Steps:
    1. Cast 'value' column from bytes to string
    2. Use from_json() to parse JSON string with the schema
    3. Select and rename columns to match your target table:
       - incident_id
       - crime_type (from narrative)
       - report_date
       - offense_date
       - address
       - latitude (cast to Double)
       - longitude (cast to Double)
       - city
       - state
    4. Add any derived columns (e.g., geom point for PostGIS)
    
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
    """
    pass


def write_to_neo4j(df):
    """
    TODO: Write crime data to Neo4j knowledge graph.
    
    This is more complex - options include:
    1. Use foreachBatch with neo4j Python driver to create nodes/relationships
    2. Use the neo4j-spark-connector (requires additional JAR)
    
    For each crime record, you might create:
    - (:Incident {id, type, date})
    - (:Location {address, lat, lon})
    - (:TimeBlock {hour, day_of_week})
    - Relationships: (Incident)-[:OCCURRED_AT]->(Location)
    
    Hint: Look at your existing etl.py for Neo4j patterns.
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
    6. Write to Neo4j (streaming)
    7. Await termination
    
    Example structure:
        spark = get_spark_session()
        schema = get_crime_schema()
        
        raw_df = read_from_kafka(spark)
        transformed_df = transform_crime_data(raw_df, schema)
        
        # Start Postgres stream
        postgres_query = write_to_postgres(transformed_df, "historical_crimes") \\
            .start()
        
        # Optionally start Neo4j stream
        # neo4j_query = write_to_neo4j(transformed_df).start()
        
        postgres_query.awaitTermination()
    """
    print("Crime Processor starting...")
    print(f"Reading from Kafka: {KAFKA_BROKER}/{TOPIC_NAME}")
    print(f"Writing to Postgres: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Writing to Neo4j: {NEO4J_URI}")
    
    # TODO: Implement the streaming pipeline
    
    print("Crime Processor skeleton - implement the TODO sections")


if __name__ == "__main__":
    process_stream()

