"""
POI Data ETL - Batch loader for OpenStreetMap Points of Interest

Loads POI data from S3 into Neo4j and creates spatial relationships
between crime incidents and nearby places.

Usage:
    cd backend
    python -m orchestration.POI_data_ETL
"""

import os
import sys
from typing import Any

from neo4j import GraphDatabase
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from s3_utils import download_data

load_dotenv()

# --- CONFIGURATION ---
NEO4J_URI = os.getenv('NEO4J_URI', 'bolt://localhost:7687')
NEO4J_USER = os.getenv('NEO4J_USER', 'neo4j')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD', 'password')

# S3 path where POI data lives
POI_S3_PATH = "raw/crime/gainesville_pois.json"


class POILoader:
    """Batch loader for POI data from S3 to Neo4j."""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print(f"Connected to Neo4j at {uri}")
    
    def close(self):
        """Close Neo4j connection."""
        self.driver.close()
        print("Neo4j connection closed.")
    
    def create_constraints(self):
        """Create indexes and constraints for Place nodes."""
        queries = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Place) REQUIRE p.osm_id IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (p:Place) ON (p.amenity_type)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Place) ON (p.lat)",
            "CREATE INDEX IF NOT EXISTS FOR (p:Place) ON (p.lon)",
        ]
        
        with self.driver.session() as session:
            for query in queries:
                session.run(query)
        print("Place node constraints and indexes created.")
    
    def load_places(self, pois: list[dict[str, Any]]) -> int:
        """
        Create Place nodes from POI data.
        
        Args:
            pois: List of POI dictionaries from S3
            
        Returns:
            Number of places created
        """
        query = """
        MERGE (p:Place {osm_id: $osm_id})
        SET p.name = $name,
            p.amenity_type = $amenity,
            p.lat = $lat,
            p.lon = $lon
        """
        
        count = 0
        with self.driver.session() as session:
            for poi in pois:
                # Skip POIs without coordinates
                if poi.get("lat") is None or poi.get("lon") is None:
                    continue
                
                session.run(
                    query,
                    osm_id=poi.get("id"),
                    name=poi.get("name", "Unknown"),
                    amenity=poi.get("amenity", "unknown"),
                    lat=float(poi.get("lat")),
                    lon=float(poi.get("lon"))
                )
                count += 1
                
                # Progress indicator every 100 POIs
                if count % 100 == 0:
                    print(f"  Loaded {count} places...")
        
        return count
    
    def create_occurred_near_relationships(self, distance_threshold: int = 200) -> int:
        """
        Create OCCURRED_NEAR relationships between incidents and nearby places.
        
        Uses Neo4j's native point.distance() function to calculate distances
        between crime locations and POIs.
        
        Args:
            distance_threshold: Maximum distance in meters (default 200m)
            
        Returns:
            Number of relationships created
        """
        # This query finds all incidents near places and creates relationships
        # We batch by amenity type to avoid memory issues with large datasets
        
        query = """
        MATCH (i:Incident)-[:OCCURRED_AT]->(l:Location)
        MATCH (p:Place)
        WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
          AND p.lat IS NOT NULL AND p.lon IS NOT NULL
        WITH i, l, p,
             point.distance(
                 point({latitude: l.lat, longitude: l.lon}),
                 point({latitude: p.lat, longitude: p.lon})
             ) AS distance_meters
        WHERE distance_meters < $threshold
        MERGE (i)-[r:OCCURRED_NEAR]->(p)
        SET r.distance_meters = distance_meters
        RETURN count(r) AS relationships_created
        """
        
        with self.driver.session() as session:
            result = session.run(query, threshold=distance_threshold)
            record = result.single()
            count = record["relationships_created"] if record else 0
        
        return count
    
    def get_stats(self) -> dict:
        """Get current counts of Place nodes and OCCURRED_NEAR relationships."""
        with self.driver.session() as session:
            place_count = session.run("MATCH (p:Place) RETURN count(p) AS count").single()["count"]
            rel_count = session.run("MATCH ()-[r:OCCURRED_NEAR]->() RETURN count(r) AS count").single()["count"]
        
        return {
            "place_nodes": place_count,
            "occurred_near_relationships": rel_count
        }
    
    def run(self, pois: list[dict[str, Any]], distance_threshold: int = 200):
        """
        Main ETL orchestration.
        
        Args:
            pois: List of POI dictionaries from S3
            distance_threshold: Maximum distance for OCCURRED_NEAR relationships
        """
        print("\n" + "=" * 50)
        print("POI Data ETL Starting...")
        print("=" * 50)
        
        # Step 1: Create constraints
        print("\n[1/3] Creating constraints and indexes...")
        self.create_constraints()
        
        # Step 2: Load Place nodes
        print(f"\n[2/3] Loading {len(pois)} POIs as Place nodes...")
        places_created = self.load_places(pois)
        print(f"  Created {places_created} Place nodes")
        
        # Step 3: Create relationships
        print(f"\n[3/3] Creating OCCURRED_NEAR relationships ({distance_threshold}m threshold)...")
        print("  This may take a while for large datasets...")
        relationships_created = self.create_occurred_near_relationships(distance_threshold)
        print(f"  Created {relationships_created} OCCURRED_NEAR relationships")
        
        # Summary
        print("\n" + "=" * 50)
        print("POI ETL Complete!")
        print("=" * 50)
        stats = self.get_stats()
        print(f"  Total Place nodes: {stats['place_nodes']}")
        print(f"  Total OCCURRED_NEAR relationships: {stats['occurred_near_relationships']}")


if __name__ == "__main__":
    print("=" * 50)
    print("POI Data ETL Loader")
    print("=" * 50)
    
    # Step 1: Download POI data from S3
    print(f"\nDownloading POI data from S3: {POI_S3_PATH}")
    poi_data = download_data(POI_S3_PATH)
    
    if poi_data is None:
        print("ERROR: Failed to download POI data from S3.")
        print("Make sure you have run the OSM ingestion first:")
        print("  python -m ingestion.ingest_osm")
        sys.exit(1)
    
    print(f"Found {len(poi_data)} POIs in S3")
    
    # Step 2: Initialize loader and run ETL
    loader = POILoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
    
    try:
        loader.run(poi_data, distance_threshold=200)
    except Exception as e:
        print(f"ERROR: ETL failed - {e}")
        sys.exit(1)
    finally:
        loader.close()
    
    print("\nDone!")
