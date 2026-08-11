import os
import boto3
from botocore.exceptions import ClientError
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

class S3StorageService:
    def __init__(self):
        # We use standard synchronous boto3 since it's running inside a Dramatiq worker thread
        # which is already decoupled from the async event loop.
        self.s3_client = boto3.client(
            's3',
            endpoint_url=settings.S3_ENDPOINT_URL,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.bucket = settings.S3_EXPORT_BUCKET
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except ClientError:
            try:
                # For MinIO / AWS us-east-1
                self.s3_client.create_bucket(Bucket=self.bucket)
            except ClientError as e:
                logger.error(f"Failed to ensure S3 bucket {self.bucket} exists: {e}")

    def upload_file(self, file_path: str, object_name: str) -> bool:
        """Upload a file to an S3 bucket"""
        try:
            self.s3_client.upload_file(file_path, self.bucket, object_name)
            return True
        except ClientError as e:
            logger.error(f"Upload failed: {e}")
            return False

    def generate_presigned_url(self, object_name: str, expiration: int = settings.S3_PRESIGNED_EXPIRY_SECONDS) -> str:
        """Generate a presigned URL to share an S3 object"""
        try:
            response = self.s3_client.generate_presigned_url('get_object',
                                                            Params={'Bucket': self.bucket,
                                                                    'Key': object_name},
                                                            ExpiresIn=expiration)
            return response
        except ClientError as e:
            logger.error(f"Failed to generate presigned URL: {e}")
            return ""
