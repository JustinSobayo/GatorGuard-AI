"""
Unit tests for s3_utils.py

Tests S3 client initialization, data upload, and download functionality.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from backend.s3_utils import get_s3_client, upload_raw_data, download_data


class TestGetS3Client:
    """Test S3 client initialization."""
    
    def test_get_s3_client_success(self, mocker):
        """Test that get_s3_client returns a boto3 client."""
        # Arrange
        mock_client = Mock()
        mocker.patch('backend.s3_utils.boto3.client', return_value=mock_client)
        
        # Act
        result = get_s3_client()
        
        # Assert
        assert result == mock_client
        
    def test_get_s3_client_uses_env_vars(self, mocker):
        """Test that client uses credentials from environment variables."""
        # Arrange
        mock_boto3 = mocker.patch('backend.s3_utils.boto3.client')
        mocker.patch('backend.s3_utils.AWS_ACCESS_KEY', 'test_key')
        mocker.patch('backend.s3_utils.AWS_SECRET_KEY', 'test_secret')
        mocker.patch('backend.s3_utils.AWS_REGION', 'us-east-1')
        
        # Act
        get_s3_client()
        
        # Assert
        mock_boto3.assert_called_once()


class TestUploadRawData:
    """Test S3 data upload functionality."""
    
    def test_upload_raw_data_success(self, mocker):
        """Test successful upload to S3."""
        # Arrange
        test_data = [{"crime": "theft", "location": "123 Main St"}]
        filename = "test_crime.json"
        
        mock_s3_client = Mock()
        mocker.patch('backend.s3_utils.get_s3_client', return_value=mock_s3_client)
        mocker.patch('backend.s3_utils.BUCKET_NAME', 'test-bucket')
        
        # Act
        result = upload_raw_data(test_data, filename)
        
        # Assert
        assert result is True
        mock_s3_client.put_object.assert_called_once()
        
    def test_upload_raw_data_correct_path(self, mocker):
        """Test that data is uploaded to correct S3 path."""
        # Arrange
        test_data = [{"test": "data"}]
        filename = "test.json"
        
        mock_s3_client = Mock()
        mocker.patch('backend.s3_utils.get_s3_client', return_value=mock_s3_client)
        mocker.patch('backend.s3_utils.BUCKET_NAME', 'test-bucket')
        
        # Act
        upload_raw_data(test_data, filename)
        
        # Assert
        call_args = mock_s3_client.put_object.call_args
        assert call_args[1]['Key'] == 'raw/crime/test.json'
        assert call_args[1]['Bucket'] == 'test-bucket'
        
    def test_upload_raw_data_handles_exception(self, mocker):
        """Test that upload returns False on exception."""
        # Arrange
        test_data = [{"test": "data"}]
        
        mock_s3_client = Mock()
        mock_s3_client.put_object.side_effect = Exception("S3 error")
        mocker.patch('backend.s3_utils.get_s3_client', return_value=mock_s3_client)
        
        # Act
        result = upload_raw_data(test_data, "test.json")
        
        # Assert
        assert result is False


class TestDownloadData:
    """Test S3 data download functionality."""
    
    def test_download_data_success(self, mocker):
        """Test successful download from S3."""
        # Arrange
        filename = "test_crime.json"
        expected_data = [{"crime": "theft"}]
        
        mock_response = {
            'Body': MagicMock()
        }
        mock_response['Body'].read.return_value = b'[{"crime": "theft"}]'
        
        mock_s3_client = Mock()
        mock_s3_client.get_object.return_value = mock_response
        mocker.patch('backend.s3_utils.get_s3_client', return_value=mock_s3_client)
        mocker.patch('backend.s3_utils.BUCKET_NAME', 'test-bucket')
        
        # Act
        result = download_data(filename)
        
        # Assert
        assert result == expected_data
        
    def test_download_data_handles_exception(self, mocker):
        """Test that download returns None on exception."""
        # Arrange
        mock_s3_client = Mock()
        mock_s3_client.get_object.side_effect = Exception("S3 error")
        mocker.patch('backend.s3_utils.get_s3_client', return_value=mock_s3_client)
        
        # Act
        result = download_data("test.json")
        
        # Assert
        assert result is None
