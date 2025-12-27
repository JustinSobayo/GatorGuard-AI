"""
Unit tests for ingest_osm.py

Tests OpenStreetMap POI data fetching from Overpass API.
"""
import pytest
from unittest.mock import Mock
import requests
from backend.ingest_osm import fetch_gainesville_pois


class TestFetchGainesvillePois:
    """Test OSM POI data fetching."""
    
    def test_fetch_gainesville_pois_success(self, mocker):
        """Test successful fetch of POI data from Overpass API."""
        # Arrange
        fake_response = {
            "elements": [
                {
                    "type": "node",
                    "id": 123456,
                    "lat": 29.6516,
                    "lon": -82.3248,
                    "tags": {"amenity": "bar", "name": "The Swamp"}
                },
                {
                    "type": "way",
                    "id": 789012,
                    "center": {"lat": 29.6520, "lon": -82.3250},
                    "tags": {"amenity": "restaurant", "name": "Gator Diner"}
                }
            ]
        }
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = fake_response
        
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert result is not None
        assert len(result) == 2
        assert result[0]["name"] == "The Swamp"
        assert result[0]["amenity"] == "bar"
        assert result[0]["lat"] == 29.6516
        assert result[1]["name"] == "Gator Diner"
        
    def test_fetch_gainesville_pois_handles_way_center(self, mocker):
        """Test that function correctly extracts center coordinates from 'way' elements."""
        # Arrange
        fake_response = {
            "elements": [
                {
                    "type": "way",
                    "id": 123,
                    "center": {"lat": 29.65, "lon": -82.32},
                    "tags": {"amenity": "parking"}
                }
            ]
        }
        
        mock_response = Mock()
        mock_response.json.return_value = fake_response
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert result[0]["lat"] == 29.65
        assert result[0]["lon"] == -82.32
        
    def test_fetch_gainesville_pois_filters_missing_coordinates(self, mocker):
        """Test that POIs without coordinates are filtered out."""
        # Arrange
        fake_response = {
            "elements": [
                {
                    "type": "node",
                    "id": 123,
                    "tags": {"amenity": "bar"}
                    # Missing lat/lon
                },
                {
                    "type": "node",
                    "id": 456,
                    "lat": 29.65,
                    "lon": -82.32,
                    "tags": {"amenity": "atm"}
                }
            ]
        }
        
        mock_response = Mock()
        mock_response.json.return_value = fake_response
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert len(result) == 1
        assert result[0]["id"] == 456
        
    def test_fetch_gainesville_pois_timeout(self, mocker):
        """Test that function returns None on timeout."""
        # Arrange
        mocker.patch(
            'backend.ingest_osm.requests.post',
            side_effect=requests.exceptions.Timeout("API timeout")
        )
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert result is None
        
    def test_fetch_gainesville_pois_http_error(self, mocker):
        """Test that function returns None on HTTP error."""
        # Arrange
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert result is None
        
    def test_fetch_gainesville_pois_empty_response(self, mocker):
        """Test handling of empty API response."""
        # Arrange
        fake_response = {"elements": []}
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = fake_response
        
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        result = fetch_gainesville_pois()
        
        # Assert
        assert result == []
        
    def test_fetch_gainesville_pois_uses_correct_timeout(self, mocker):
        """Test that API call uses appropriate timeout."""
        # Arrange
        mock_response = Mock()
        mock_response.json.return_value = {"elements": []}
        mock_post = mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        fetch_gainesville_pois()
        
        # Assert
        call_args = mock_post.call_args
        assert call_args[1]['timeout'] == 120
        
    def test_fetch_gainesville_pois_logs_count(self, mocker, capsys):
        """Test that function logs the number of POIs fetched."""
        # Arrange
        fake_response = {
            "elements": [
                {"type": "node", "id": 1, "lat": 29.65, "lon": -82.32, "tags": {"amenity": "bar"}},
                {"type": "node", "id": 2, "lat": 29.66, "lon": -82.33, "tags": {"amenity": "atm"}}
            ]
        }
        
        mock_response = Mock()
        mock_response.json.return_value = fake_response
        mocker.patch('backend.ingest_osm.requests.post', return_value=mock_response)
        
        # Act
        fetch_gainesville_pois()
        
        # Assert
        captured = capsys.readouterr()
        assert "Successfully fetched 2 POIs" in captured.out
