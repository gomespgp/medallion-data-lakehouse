import os
import io
from typing import Optional
from minio import Minio


class AwsS3:
    """Wrapper client for MinIO / S3-compatible object storage operations."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: bool = False,
    ):
        self.endpoint = endpoint or os.getenv("MINIO_ENDPOINT")
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD")
        self.secure = secure

        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )

    def ensure_bucket_exists(self, bucket_name: str) -> None:
        """Creates the bucket if it does not already exist."""
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def upload_stream(
        self,
        bucket_name: str,
        object_path: str,
        buffer: io.BytesIO,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Uploads a BytesIO stream to MinIO/S3 and returns the full S3 URI."""
        self.ensure_bucket_exists(bucket_name)

        buffer.seek(0)
        content_length = buffer.getbuffer().nbytes

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_path,
            data=buffer,
            length=content_length,
            content_type=content_type,
        )

        return f"s3://{bucket_name}/{object_path}"