import logging
import json
import os
import hashlib
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from kafka import KafkaProducer

try:
    from backend.s3_utils import download_data
except ImportError:  # pragma: no cover - supports direct execution from backend/
    import sys

    BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if BACKEND_DIR not in sys.path:
        sys.path.insert(0, BACKEND_DIR)
    from s3_utils import download_data

load_dotenv()

# --- LOGGING CONFIG ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER') or os.getenv('KAFKA_BROKER_URL', 'localhost:29092')
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


def _first(record: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in record and record[key] not in (None, ""):
            return record[key]
    return None


def _to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_coordinates(record: dict[str, Any]) -> tuple[float | None, float | None]:
    lat = _to_float(_first(record, "latitude", "lat", "y", "offense_latitude"))
    lon = _to_float(_first(record, "longitude", "lon", "lng", "x", "offense_longitude"))
    if lat is not None and lon is not None:
        return lat, lon

    for key in ("geocoded_column", "location", "point"):
        value = record.get(key)
        if not isinstance(value, dict):
            continue

        nested_lat = _to_float(_first(value, "latitude", "lat"))
        nested_lon = _to_float(_first(value, "longitude", "lon", "lng"))
        if nested_lat is not None and nested_lon is not None:
            return nested_lat, nested_lon

        coordinates = value.get("coordinates")
        if isinstance(coordinates, list) and len(coordinates) >= 2:
            nested_lon = _to_float(coordinates[0])
            nested_lat = _to_float(coordinates[1])
            if nested_lat is not None and nested_lon is not None:
                return nested_lat, nested_lon

    return None, None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _stable_record_id(record: dict[str, Any]) -> str:
    raw = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _as_text(value: Any, default: str = "") -> str:
    if value in (None, ""):
        return default
    if isinstance(value, dict):
        return str(_first(value, "human_address", "address", "name") or default)
    return str(value)


def normalize_crime_record(record: dict[str, Any]) -> dict[str, Any] | None:
    """Map Gainesville/Socrata field variants into the Spark processor schema."""
    lat, lon = _extract_coordinates(record)
    if lat is None or lon is None:
        return None

    offense_date_raw = _first(
        record,
        "offense_date",
        "incident_date",
        "date",
        "occurred_date",
        "offense_datetime",
        "incident_datetime",
    )
    report_date_raw = _first(record, "report_date", "reported_date", "date_reported", "report_datetime")

    offense_dt = _parse_datetime(offense_date_raw)
    report_dt = _parse_datetime(report_date_raw)
    hour_value = _first(record, "offense_hour_of_day", "offense_hour", "hour")
    if hour_value in (None, "") and offense_dt is not None:
        hour_value = offense_dt.hour

    day_value = _first(record, "offense_day_of_week", "day_of_week", "incident_day_of_week")
    if not day_value and offense_dt is not None:
        day_value = offense_dt.strftime("%A")

    incident_type = _first(
        record,
        "incident_type",
        "narrative",
        "crime_type",
        "offense_type",
        "offense",
        "description",
    ) or "unknown"

    address = _as_text(
        _first(record, "address", "location", "incident_address", "block_address"),
        default="Unknown location",
    )

    return {
        "id": str(_first(record, "id", "incident_id", "case_number", "case_id") or _stable_record_id(record)),
        "incident_type": str(incident_type),
        "description": str(_first(record, "description", "narrative", "offense_description") or incident_type),
        "report_date": report_dt.isoformat() if report_dt else str(report_date_raw or ""),
        "offense_date": offense_dt.isoformat() if offense_dt else str(offense_date_raw or ""),
        "offense_hour_of_day": str(hour_value) if hour_value not in (None, "") else None,
        "offense_day_of_week": str(day_value) if day_value else None,
        "address": address,
        "latitude": str(lat),
        "longitude": str(lon),
        "city": _as_text(_first(record, "city"), default="Gainesville"),
        "state": _as_text(_first(record, "state"), default="FL"),
    }


def normalize_crime_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for record in records:
        normalized_record = normalize_crime_record(record)
        if normalized_record:
            normalized.append(normalized_record)
    return normalized

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
    raw_records = fetch_crime_data_from_s3()
    
    if raw_records:
        records = normalize_crime_records(raw_records)
        logger.info(f"Normalized {len(records)}/{len(raw_records)} geocoded crime records for Kafka.")
        count = send_to_kafka(producer, records)
        logger.info(f"Successfully sent {count} records to Kafka.")
    else:
        logger.warning("No records found to process.")
        
    producer.close()

if __name__ == "__main__":
    run()


