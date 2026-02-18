"""S3 Uploader for Bronze Layer Backup.

This module provides functionality to:
1. Create tar archives of bronze layer data
2. Upload to S3 (Arvan Cloud) with proper naming convention

Naming convention: {scraper}/YYYYMM/YYYYMMDD.tar.gz
Example: fotmob/202509/20250901.tar.gz, aiscore/202509/20250915.tar.gz
"""

import os
import tarfile
import tempfile
from pathlib import Path
from typing import Optional, List

try:
    import boto3
    from botocore.config import Config as BotoConfig
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

from ..utils.logging_utils import get_logger


class S3Uploader:
    """S3 Uploader for bronze layer data."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str = "scout-sport"
    ):
        """Initialize S3 uploader."""
        self.logger = get_logger()
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        
        if not BOTO3_AVAILABLE:
            self.logger.warning("boto3 not installed, S3 upload will be disabled")
            self.s3_client = None
            return

        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=BotoConfig(signature_version='s3v4'),
            region_name='ir-tbz-sh1'
        )
        self.logger.info(f"S3 uploader initialized for bucket: {bucket_name}")

    def list_existing_dates(self, scraper_name: str, year_month: str) -> List[str]:
        """List dates already uploaded for a given month."""
        if not self.s3_client:
            return []
        
        try:
            prefix = f"bronze/{scraper_name}/{year_month}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if response.get('Contents'):
                return [
                    obj['Key'].split('/')[-1].replace('.tar.gz', '')
                    for obj in response['Contents']
                ]
        except Exception as e:
            self.logger.warning(f"Failed to list existing objects: {e}")
        
        return []

    def create_tar_archive(
        self,
        source_dir: str,
        date_str: str,
        scraper_name: str
    ) -> Optional[str]:
        """Create a tar archive of bronze layer data for a specific date."""
        try:
            year_month = date_str[:6]
            tar_filename = f"{date_str}.tar.gz"
            
            source_path = Path(source_dir)
            if not source_path.exists():
                self.logger.warning(f"Source directory does not exist: {source_dir}")
                return None

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                tar_path = temp_path / tar_filename
                
                self.logger.info(f"Creating tar archive: {tar_filename}")
                
                with tarfile.open(tar_path, "w:gz") as tar:
                    tar.add(source_path, arcname=date_str)
                
                self.logger.info(f"Tar archive created: {tar_path}")
                return str(tar_path)

        except Exception as e:
            self.logger.error(f"Failed to create tar archive: {e}")
            return None

    def upload_to_s3(
        self,
        local_file: str,
        s3_key: str
    ) -> bool:
        """Upload file to S3."""
        if not self.s3_client:
            self.logger.error("S3 client not initialized")
            return False

        try:
            self.s3_client.upload_file(
                local_file,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': 'application/gzip'
                }
            )
            self.logger.info(f"Uploaded to S3: {s3_key}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to upload to S3: {e}")
            return False

    def upload_bronze_backup(
        self,
        bronze_dir: str,
        date_str: str,
        scraper_name: str
    ) -> bool:
        """Create and upload bronze layer backup for a specific date.
        
        Saves as: bronze/{scraper}/YYYYMM/YYYYMMDD.tar.gz

        Args:
            bronze_dir: Bronze layer directory path (e.g., data/fotmob/20250901)
            date_str: Date string (YYYYMMDD)
            scraper_name: Name of scraper (fotmob or aiscore)

        Returns:
            True if successful, False otherwise
        """
        year_month = date_str[:6]
        s3_key = f"bronze/{scraper_name}/{year_month}/{date_str}.tar.gz"
        
        self.logger.info(f"Starting bronze backup upload for {scraper_name} on {date_str}")
        
        tar_path = self.create_tar_archive(bronze_dir, date_str, scraper_name)
        if not tar_path:
            self.logger.error("Failed to create tar archive")
            return False
        
        success = self.upload_to_s3(tar_path, s3_key)
        
        try:
            os.remove(tar_path)
        except:
            pass
        
        return success


def get_s3_uploader() -> Optional[S3Uploader]:
    """Get S3 uploader instance from environment variables."""
    endpoint = os.getenv('S3_ENDPOINT')
    access_key = os.getenv('S3_ACCESS_KEY')
    secret_key = os.getenv('S3_SECRET_KEY')
    
    if not all([endpoint, access_key, secret_key]):
        get_logger().warning("S3 not fully configured, skipping upload")
        return None
    
    if access_key == 'your_access_key_here':
        get_logger().warning("S3 credentials not configured, skipping upload")
        return None
    
    return S3Uploader(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key
    )
