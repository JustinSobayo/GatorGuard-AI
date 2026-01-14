import os
import requests
import json
from typing import Any
from dotenv import load_dotenv
from backend.s3_utils import upload_raw_data

load_dotenv()

# SODA3 API Configuration
API_URL = os.getenv("GAINESVILLE_API_URL")
APP_TOKEN = os.getenv("GAINESVILLE_APP_TOKEN")
SECRET_TOKEN = os.getenv("GAINESVILLE_SECRET_TOKEN")

def fetch_historical_crimes() -> list[dict[str, Any]] | None:
    """
    Fetch all historical crime data from Gainesville Open Data Portal.
    
    SODA3 uses standard pagination if needed, but we'll try to fetch a large batch.
    
    Returns:
        List of crime records as dictionaries, or None if fetch failed
    """
    print(f"Connecting to {API_URL}...")
    headers = {
        "X-App-Token": APP_TOKEN,
        # SODA3 can use basic auth with Key/Secret if configured, 
        # but for public datasets, the App Token is usually enough for data science access.
    }
    
    # SODA3 Query for all data (SoQL)
    # limit=300000 ensures we get the ~226k rows in one go if possible
    params = {
        "$limit": 300000 
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        count = len(data)
        print(f"Successfully fetched {count} crime records.")
        return data
    except Exception as e:
        print(f"Error fetching data from Gainesville API: {str(e)}")
        return None

if __name__ == "__main__":
    # 1. Fetch
    crime_data = fetch_historical_crimes()
    
    if crime_data:
        # 2. Upload to S3
        print("Starting upload to S3 Data Lake...")
        success = upload_raw_data(crime_data, "historical_crime_full.json")
        
        if success:
            print("--- DATA INGESTION COMPLETE ---")
            print("The 'Bronze Layer' is now populated. Next step: Spark ETL.")
        else:
            print("Failed to upload data to S3.")
    else:
        print("No data fetched. Check your GAINESVILLE_APP_TOKEN and network connection.")
