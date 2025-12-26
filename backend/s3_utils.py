import boto3
import json
import os
from typing import Any
from dotenv import load_dotenv
from botocore.client import BaseClient

load_dotenv()

# AWS Credentials from .env
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

def get_s3_client() -> BaseClient:
    """
    Initialize and return a boto3 S3 client using credentials from .env.
    
    Returns:
        Configured S3 client instance
    """
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

def upload_raw_data(data: list[dict[str, Any]], filename: str) -> bool:
    """
    Upload raw JSON data to S3 into the 'raw/crime/' folder.
    
    Args:
        data: List of crime records as dictionaries
        filename: Target filename in S3 bucket
        
    Returns:
        True if upload succeeded, False otherwise
    """
    s3 = get_s3_client()
    try:
        json_data = json.dumps(data)
        # We store in a subfolder structure for better data lake organization
        target_path = f"raw/crime/{filename}"
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=target_path,
            Body=json_data,
            ContentType='application/json'
        )
        print(f"Successfully uploaded {filename} to s3://{BUCKET_NAME}/{target_path}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return False

def download_data(filename: str) -> list[dict[str, Any]] | None:
    """
    Download data from S3 for processing.
    
    Args:
        filename: Path to file in S3 bucket
        
    Returns:
        Parsed JSON data as list of dictionaries, or None if error
    """
    s3 = get_s3_client()
    try:
        response = s3.get_object(Bucket=BUCKET_NAME, Key=filename)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        print(f"Error downloading from S3: {str(e)}")
        return None
