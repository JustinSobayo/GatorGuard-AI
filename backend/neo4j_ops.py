"""
Neo4j Operations - Query layer for crime explanation knowledge graph

Provides fast queries for the /predict/explain endpoint.
Uses connection pooling for performance.
"""

import os
import time
from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')

# --- CONNECTION POOLING ---
_driver = None


def get_driver():
    """
    Get or create the Neo4j driver (singleton pattern for connection pooling).
    
    Returns:
        Neo4j driver instance
    """
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver


def close_driver():
    """Close the Neo4j driver connection."""
    global _driver
    if _driver:
        _driver.close()
        _driver = None


# --- HELPER FUNCTIONS ---

def _parse_grid_id(grid_id: str) -> tuple[float, float]:
    """
    Parse a grid_id to extract latitude and longitude.
    
    Supports formats:
    - "grid_29.65_-82.32" -> (29.65, -82.32)
    - "29.65,-82.32" -> (29.65, -82.32)
    - "29.65_-82.32" -> (29.65, -82.32)
    
    Args:
        grid_id: Grid identifier string
        
    Returns:
        Tuple of (latitude, longitude)
        
    Raises:
        ValueError: If grid_id cannot be parsed
    """
    try:
        # Remove "grid_" prefix if present
        clean_id = grid_id.replace("grid_", "")
        
        # Try splitting by underscore first, then comma
        if "_" in clean_id:
            parts = clean_id.split("_")
        elif "," in clean_id:
            parts = clean_id.split(",")
        else:
            raise ValueError(f"Cannot parse grid_id: {grid_id}")
        
        if len(parts) != 2:
            raise ValueError(f"Expected 2 coordinates, got {len(parts)}: {grid_id}")
        
        lat = float(parts[0].strip())
        lon = float(parts[1].strip())
        
        return lat, lon
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid grid_id format '{grid_id}': {e}")


def _get_crime_counts(session, lat: float, lon: float, radius: int) -> list[dict[str, Any]]:
    """
    Get crime counts by type within a radius of the specified location.
    
    Args:
        session: Neo4j session
        lat: Center latitude
        lon: Center longitude
        radius: Radius in meters
        
    Returns:
        List of dicts with crime_type and count
    """
    query = """
    MATCH (i:Incident)-[:OCCURRED_AT]->(l:Location)
    WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
      AND point.distance(
          point({latitude: l.lat, longitude: l.lon}),
          point({latitude: $center_lat, longitude: $center_lon})
      ) < $radius_meters
    RETURN i.type AS crime_type, count(i) AS count
    ORDER BY count DESC
    LIMIT 10
    """
    
    result = session.run(query, center_lat=lat, center_lon=lon, radius_meters=radius)
    return [{"type": record["crime_type"], "count": record["count"]} for record in result]


def _get_temporal_patterns(session, lat: float, lon: float, radius: int) -> dict[str, Any]:
    """
    Get temporal patterns (time of day, day of week) for crimes in the area.
    
    Args:
        session: Neo4j session
        lat: Center latitude
        lon: Center longitude
        radius: Radius in meters
        
    Returns:
        Dict with peak times and breakdowns by day/time
    """
    query = """
    MATCH (i:Incident)-[:OCCURRED_AT]->(l:Location),
          (i)-[:OCCURRED_DURING]->(tb:TimeBlock),
          (i)-[:ON_DAY]->(d:Day)
    WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
      AND point.distance(
          point({latitude: l.lat, longitude: l.lon}),
          point({latitude: $center_lat, longitude: $center_lon})
      ) < $radius_meters
    RETURN tb.part_of_day AS time_of_day, tb.hour AS hour, d.name AS day, count(i) AS count
    ORDER BY count DESC
    """
    
    result = session.run(query, center_lat=lat, center_lon=lon, radius_meters=radius)
    records = list(result)
    
    if not records:
        return {
            "peak_hour": None,
            "peak_day": None,
            "peak_time_of_day": None,
            "by_day": {},
            "by_time": {}
        }
    
    # Aggregate by day
    by_day = {}
    for record in records:
        day = record["day"]
        if day:
            by_day[day] = by_day.get(day, 0) + record["count"]
    
    # Aggregate by time of day
    by_time = {}
    for record in records:
        time_of_day = record["time_of_day"]
        if time_of_day:
            by_time[time_of_day] = by_time.get(time_of_day, 0) + record["count"]
    
    # Find peaks
    peak_day = max(by_day, key=by_day.get) if by_day else None
    peak_time = max(by_time, key=by_time.get) if by_time else None
    peak_hour = records[0]["hour"] if records else None
    
    return {
        "peak_hour": peak_hour,
        "peak_day": peak_day,
        "peak_time_of_day": peak_time,
        "by_day": by_day,
        "by_time": by_time
    }


def _get_nearby_places_with_crimes(session, lat: float, lon: float, radius: int) -> list[dict[str, Any]]:
    """
    Get nearby places (POIs) ranked by incident count.
    
    Args:
        session: Neo4j session
        lat: Center latitude
        lon: Center longitude
        radius: Radius in meters
        
    Returns:
        List of dicts with place name, type, and incident count
    """
    query = """
    MATCH (i:Incident)-[:OCCURRED_NEAR]->(p:Place)
    MATCH (i)-[:OCCURRED_AT]->(l:Location)
    WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
      AND point.distance(
          point({latitude: l.lat, longitude: l.lon}),
          point({latitude: $center_lat, longitude: $center_lon})
      ) < $radius_meters
    RETURN p.name AS place_name, p.amenity_type AS place_type, count(i) AS incident_count
    ORDER BY incident_count DESC
    LIMIT 10
    """
    
    result = session.run(query, center_lat=lat, center_lon=lon, radius_meters=radius)
    return [
        {
            "name": record["place_name"],
            "type": record["place_type"],
            "incident_count": record["incident_count"]
        }
        for record in result
    ]


def _get_seasonal_patterns(session, lat: float, lon: float, radius: int) -> dict[str, int]:
    """
    Get crime counts by season for the area.
    
    Args:
        session: Neo4j session
        lat: Center latitude
        lon: Center longitude
        radius: Radius in meters
        
    Returns:
        Dict mapping season name to count
    """
    query = """
    MATCH (i:Incident)-[:OCCURRED_AT]->(l:Location),
          (i)-[:DURING_SEASON]->(s:Season)
    WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
      AND point.distance(
          point({latitude: l.lat, longitude: l.lon}),
          point({latitude: $center_lat, longitude: $center_lon})
      ) < $radius_meters
    RETURN s.name AS season, count(i) AS count
    ORDER BY count DESC
    """
    
    result = session.run(query, center_lat=lat, center_lon=lon, radius_meters=radius)
    return {record["season"]: record["count"] for record in result}


# --- MAIN EXPORT ---

def get_crime_explanation(grid_id: str, radius_meters: int = 500) -> dict[str, Any]:
    """
    Get comprehensive crime explanation data for a location.
    
    This function queries the Neo4j knowledge graph to gather context
    that can be passed to an LLM for generating natural language explanations.
    
    Args:
        grid_id: Grid identifier (e.g., "grid_29.65_-82.32" or "29.65,-82.32")
        radius_meters: Search radius in meters (default 500m)
        
    Returns:
        Dict containing:
        - grid_id: The input grid ID
        - center: Dict with lat/lon coordinates
        - radius_meters: The search radius used
        - total_incidents: Total crime count in area
        - crime_breakdown: List of crime types with counts
        - temporal_patterns: Peak times and day/time breakdowns
        - nearby_places: POIs ranked by incident count
        - seasonal_pattern: Crimes by season
        - query_time_ms: Time taken for queries (for monitoring)
    """
    start_time = time.time()
    
    try:
        # Parse grid_id to get coordinates
        lat, lon = _parse_grid_id(grid_id)
    except ValueError as e:
        return {
            "error": str(e),
            "grid_id": grid_id,
            "center": None,
            "radius_meters": radius_meters,
            "total_incidents": 0,
            "crime_breakdown": [],
            "temporal_patterns": {},
            "nearby_places": [],
            "seasonal_pattern": {},
            "query_time_ms": 0
        }
    
    try:
        driver = get_driver()
        
        with driver.session() as session:
            # Run all queries
            crime_counts = _get_crime_counts(session, lat, lon, radius_meters)
            temporal = _get_temporal_patterns(session, lat, lon, radius_meters)
            places = _get_nearby_places_with_crimes(session, lat, lon, radius_meters)
            seasonal = _get_seasonal_patterns(session, lat, lon, radius_meters)
        
        # Calculate total incidents
        total_incidents = sum(c["count"] for c in crime_counts)
        
        query_time_ms = round((time.time() - start_time) * 1000, 2)
        
        return {
            "grid_id": grid_id,
            "center": {"lat": lat, "lon": lon},
            "radius_meters": radius_meters,
            "total_incidents": total_incidents,
            "crime_breakdown": crime_counts,
            "temporal_patterns": temporal,
            "nearby_places": places,
            "seasonal_pattern": seasonal,
            "query_time_ms": query_time_ms
        }
        
    except ServiceUnavailable as e:
        return {
            "error": f"Neo4j connection failed: {e}",
            "grid_id": grid_id,
            "center": {"lat": lat, "lon": lon},
            "radius_meters": radius_meters,
            "total_incidents": 0,
            "crime_breakdown": [],
            "temporal_patterns": {},
            "nearby_places": [],
            "seasonal_pattern": {},
            "query_time_ms": round((time.time() - start_time) * 1000, 2)
        }
    except AuthError as e:
        return {
            "error": f"Neo4j authentication failed: {e}",
            "grid_id": grid_id,
            "center": {"lat": lat, "lon": lon},
            "radius_meters": radius_meters,
            "total_incidents": 0,
            "crime_breakdown": [],
            "temporal_patterns": {},
            "nearby_places": [],
            "seasonal_pattern": {},
            "query_time_ms": round((time.time() - start_time) * 1000, 2)
        }
