"""
Crime Data Producer

Reads historical crime data from S3 and sends it to a Kafka topic
for downstream processing by the crime_processor.

Data Flow: S3 (historical_crime_full.json) --> Kafka (raw_crime_data topic)
"""

import json
import os
from typing import Any

from dotenv import load_dotenv

# TODO: Uncomment after installing kafka-python or confluent-kafka
from kafka import KafkaProducer

from backend.s3_utils import download_data

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'
S3_CRIME_DATA_PATH = 'raw/crime/historical_crime_full.json'


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
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def fetch_crime_data_from_s3() -> list[dict[str, Any]] | None:
    """
    TODO: Fetch crime data from S3 using the s3_utils module.
    
    Steps:
    1. Use download_data() from s3_utils to fetch the JSON file
    2. Return the list of crime records
    
    Hint: download_data() already returns parsed JSON as a list of dicts.
    """
    try:
        return download_data(S3_CRIME_DATA_PATH)
    except Exception as e:
        print(f"Error fetching crime data from S3: {str(e)}")
        return None

def send_to_kafka(producer, records: list[dict[str, Any]]) -> int:
    """
    TODO: Send each crime record to the Kafka topic.
    
    Args:
        producer: The Kafka producer instance
        records: List of crime record dictionaries
        
    Returns:
        Number of records sent
        
    Steps:
    1. Loop through each record
    2. Call producer.send(TOPIC_NAME, value=record)
    3. Optionally call producer.flush() at the end
    4. Return count of records sent
    """
    count : int = 0
    try:
        for record in records:
            producer.send(TOPIC_NAME, value=record)
            count += 1
        producer.flush()
        return count
    except Exception as e:
        print(f"Error sending to Kafka: {str(e)}")
        return 0



def run():
    """
    Main execution function.
    
    TODO: Implement the full pipeline:
    1. Initialize Kafka producer using get_kafka_producer()
    2. Fetch crime data from S3 using fetch_crime_data_from_s3()
    3. Send records to Kafka using send_to_kafka()
    4. Print summary (records sent, any errors)
    5. Close the producer
    """
    print("Crime Producer starting...")
    print(f"Target Kafka broker: {KAFKA_BROKER}")
    print(f"Target topic: {TOPIC_NAME}")
    print(f"S3 source: {S3_CRIME_DATA_PATH}")

    producer = get_kafka_producer()
    records = fetch_crime_data_from_s3()
    if records:
        count = send_to_kafka(producer, records)
        print(f"Sent {count} crime records to Kafka")
    producer.close()

if __name__ == "__main__":
    run()

