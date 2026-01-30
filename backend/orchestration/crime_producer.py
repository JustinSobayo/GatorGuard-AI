import logging
import json
import os
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer
from backend.s3_utils import download_data

load_dotenv()

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:29092')
TOPIC_NAME = 'raw_crime_data'
S3_CRIME_DATA_PATH = 'raw/crime/historical_crime_full.json'


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def fetch_crime_data_from_s3() -> list[dict[str, Any]] | None:
    try:
        logger.info(f"Downloading data from S3: {S3_CRIME_DATA_PATH}...")
        return download_data(S3_CRIME_DATA_PATH)
    except Exception as e:
        logger.error(f"Error fetching crime data from S3: {str(e)}")
        return None

def send_to_kafka(producer, records: list[dict[str, Any]]) -> int:
    count : int = 0
    batch_size = 5000  # Log every 5000 records
    
    try:
        logger.info(f"Starting ingestion of {len(records)} records into Kafka topic: {TOPIC_NAME}")
        
        for record in records:
            producer.send(TOPIC_NAME, value=record)
            count += 1
            
            if count % batch_size == 0:
                logger.info(f"Progress: Sent {count}/{len(records)} records...")
                
        producer.flush()
        return count
    except Exception as e:
        logger.error(f"Error sending to Kafka: {str(e)}")
        return 0


def run():
    logger.info("Crime Producer initializing...")
    logger.info(f"Target Kafka broker: {KAFKA_BROKER}")

    producer = get_kafka_producer()
    records = fetch_crime_data_from_s3()
    
    if records:
        count = send_to_kafka(producer, records)
        logger.info(f"Successfully sent {count} records to Kafka.")
    else:
        logger.warning("No records found to process.")
        
    producer.close()

if __name__ == "__main__":
    run()


