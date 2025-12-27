import os
import requests
import json
from typing import Any
from dotenv import load_dotenv
from backend.s3_utils import upload_raw_data

load_dotenv()

# Overpass API Configuration
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Gainesville bounding box (lat/lon coordinates)
# Format: [south, west, north, east]
GAINESVILLE_BBOX = [29.5900, -82.4200, 29.7200, -82.2700]

def fetch_gainesville_pois() -> list[dict[str, Any]] | None:
    """
    Fetch Points of Interest (POIs) from OpenStreetMap for Gainesville, FL.
    
    Uses the Overpass API to query for places like bars, restaurants, ATMs,
    schools, and other locations relevant to crime pattern analysis.
    
    Returns:
        List of POI records as dictionaries, or None if fetch failed
    """
    print(f"Connecting to Overpass API...")
    
    # Overpass QL (Query Language) to fetch POIs
    # This queries for amenities (bars, restaurants, ATMs, etc.) in Gainesville
    overpass_query = f"""
    [out:json][timeout:60];
    (
      node["amenity"~"bar|pub|nightclub|restaurant|cafe|atm|bank|school|university|parking"]
        ({GAINESVILLE_BBOX[0]},{GAINESVILLE_BBOX[1]},{GAINESVILLE_BBOX[2]},{GAINESVILLE_BBOX[3]});
      way["amenity"~"bar|pub|nightclub|restaurant|cafe|atm|bank|school|university|parking"]
        ({GAINESVILLE_BBOX[0]},{GAINESVILLE_BBOX[1]},{GAINESVILLE_BBOX[2]},{GAINESVILLE_BBOX[3]});
    );
    out center;
    """
    #try making a HTTP request to the Overpass API and print the responce and if it doesn't
    #work, print there was an error
    try:
        response = requests.post(
            OVERPASS_URL,
            data={"data": overpass_query},
            timeout=120  # Overpass can be slow, give it 2 minutes
        )
        response.raise_for_status()
        data = response.json()
        
        # Extract elements from Overpass response
        elements = data.get("elements", [])
        
        # Transform to a cleaner format
        pois = []
        for element in elements:
            poi = {
                "id": element.get("id"),
                "type": element.get("type"),
                "amenity": element.get("tags", {}).get("amenity"),
                "name": element.get("tags", {}).get("name", "Unknown"),
                "lat": element.get("lat") or element.get("center", {}).get("lat"),
                "lon": element.get("lon") or element.get("center", {}).get("lon"),
            }
            # Only include POIs with valid coordinates
            if poi["lat"] and poi["lon"]:
                pois.append(poi)
        
        count = len(pois)
        print(f"Successfully fetched {count} POIs from OpenStreetMap.")
        return pois
    except Exception as e:
        print(f"Error fetching data from Overpass API: {str(e)}")
        return None

if __name__ == "__main__":
    # 1. Fetch OSM POIs
    poi_data = fetch_gainesville_pois()
    
    if poi_data:
        # 2. Upload to S3 (different folder than crime data)
        print("Starting upload to S3 Data Lake...")
        success = upload_raw_data(poi_data, "../places/gainesville_pois.json")
        
        if success:
            print("--- OSM DATA INGESTION COMPLETE ---")
            print(f"Fetched {len(poi_data)} points of interest.")
            print("Next step: Define SQL schema to load both crime and POI data.")
        else:
            print("Failed to upload POI data to S3.")
    else:
        print("No POI data fetched. Check your network connection.")
