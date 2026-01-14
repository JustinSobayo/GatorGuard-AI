"""
POI (Points of Interest) Data Producer

Reads POI data from S3 (originally from OpenStreetMap) and sends it 
to a Kafka topic for downstream processing by the poi_processor.

Data Flow: S3 (gainesville_pois.json) --> Kafka (poi_data topic)
"""

import json
import os
from typing import Any

from dotenv import load_dotenv

# TODO: Uncomment after installing kafka-python or confluent-kafka
# from kafka import KafkaProducer

from backend.s3_utils import download_data

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'poi_data'
S3_POI_DATA_PATH = 'raw/places/gainesville_pois.json'


def get_kafka_producer():
    """
    TODO: Initialize and return a Kafka Producer.
    
    Steps:
    1. Import KafkaProducer from kafka-python (or use confluent-kafka)
    2. Create producer with bootstrap_servers=KAFKA_BROKER
    3. Configure value_serializer to encode JSON to bytes
    
    Example:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    """
    pass


def fetch_poi_data_from_s3() -> list[dict[str, Any]] | None:
    """
    TODO: Fetch POI data from S3 using the s3_utils module.
    
    Steps:
    1. Use download_data() from s3_utils to fetch the JSON file
    2. Return the list of POI records
    
    Hint: download_data() already returns parsed JSON as a list of dicts.
    
    Note: The POI data contains fields like:
        - id: OSM element ID
        - type: node or way
        - amenity: bar, restaurant, atm, school, etc.
        - name: Name of the place
        - lat, lon: Coordinates
    """
    pass


def send_to_kafka(producer, records: list[dict[str, Any]]) -> int:
    """
    TODO: Send each POI record to the Kafka topic.
    
    Args:
        producer: The Kafka producer instance
        records: List of POI record dictionaries
        
    Returns:
        Number of records sent
        
    Steps:
    1. Loop through each record
    2. Call producer.send(TOPIC_NAME, value=record)
    3. Optionally call producer.flush() at the end
    4. Return count of records sent
    """
    pass


def run():
    """
    Main execution function.
    
    TODO: Implement the full pipeline:
    1. Initialize Kafka producer using get_kafka_producer()
    2. Fetch POI data from S3 using fetch_poi_data_from_s3()
    3. Send records to Kafka using send_to_kafka()
    4. Print summary (records sent, any errors)
    5. Close the producer
    
    Note: POI data updates less frequently than crime data,
    so this producer may be run on a different schedule.
    """
    print("POI Producer starting...")
    print(f"Target Kafka broker: {KAFKA_BROKER}")
    print(f"Target topic: {TOPIC_NAME}")
    print(f"S3 source: {S3_POI_DATA_PATH}")
    
    # TODO: Implement the pipeline
    # producer = get_kafka_producer()
    # records = fetch_poi_data_from_s3()
    # if records:
    #     count = send_to_kafka(producer, records)
    #     print(f"Sent {count} POI records to Kafka")
    # producer.close()
    
    print("POI Producer skeleton - implement the TODO sections")


if __name__ == "__main__":
    run()

