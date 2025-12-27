"""
Unit tests for ingest_historical.py

Tests historical crime data fetching from Gainesville Data API.
"""
import pytest
from unittest.mock import Mock, patch
import requests
from backend.ingest_historical import fetch_historical_crimes

class TestFetchHistoricalCrimes:
    """Test suite for ingest_historical functionality."""
    
    def test_fetch_historical_crimes_success(self, mocker):
        """Test successful fetch of historical crime data."""
        # Arrange
        fake_response = [
            {
            "incident_id": "12345", 
            "incident_type": "Theft",
            "incident_date": "2024-01-15",
            "latitude": "28.6929",
            "longitude": "-82.3248"
            },
            {
            "incident_id": "67890", 
            "incident_type": "Assault",
            "incident_date": "2024-01-16",
            "latitude": "29.6520",
            "longitude": "-82.3250"
            },
        ]
        
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = fake_response

        mocker.patch('backend.ingest_historical.requests.get', return_value=mock_response)
        # Act
        result = fetch_historical_crimes()

        # Assert
        assert result is not None
        assert len(result) == 2
        assert result[0]["incident_id"] == "12345"
        assert result[0]["incident_type"] == "Theft"
        assert result[0]["incident_date"] == "2024-01-15"
        assert result[0]["latitude"] == "28.6929"
        assert result[0]["longitude"] == "-82.3248"
        


        