"""S3 configuration, client, and data store identification."""

import hashlib
from dataclasses import dataclass
from typing import Dict, Iterator

try:
    import boto3
    from botocore.config import Config as BotoConfig
except ImportError:
    boto3 = None


@dataclass(frozen=True)
class S3Config:
    """Immutable S3 connection configuration.

    Replaces the loose ``s3_args`` dict that was previously threaded through
    every function.  Being frozen guarantees that credentials are never
    accidentally mutated after parsing.
    """

    bucket: str
    region: str
    access_key: str
    secret_key: str
    endpoint: str = ""  # full URL with scheme, e.g. "http://minio:9090"
    path_style: bool = False
    url_expiration: int = 10080  # minutes (7 days)

    @classmethod
    def from_data_store_info(cls, info) -> "S3Config":
        """Build S3Config from a DataStoreInfo (includes decrypted credentials)."""
        return cls(
            bucket=info.bucket_name,
            region=info.region,
            access_key=info.access_key,
            secret_key=info.secret_key,
            endpoint=info.endpoint,
            path_style=not info.use_virtual_addressing,
            url_expiration=info.url_expiration_minutes,
        )

    @property
    def store_id(self) -> str:
        """Deterministic data store name derived from bucket config.

        Two invocations with the same bucket/region/endpoint always produce the
        same name so the store can be reused or rekeyed transparently.
        """
        raw = f"{self.bucket}|{self.region}|{self.endpoint}"
        digest = hashlib.sha256(raw.encode()).hexdigest()[:12]
        safe_bucket = self.bucket.replace(".", "-")[:24]
        return f"ct-{safe_bucket}-{digest}"


class S3Client:
    """Thin wrapper around boto3 for listing and metadata operations."""

    def __init__(
        self,
        config: S3Config,
        connect_timeout: int | None = None,
        read_timeout: int | None = None,
    ):
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 operations: pip install boto3")

        boto_config_kwargs: Dict = {}
        if config.path_style:
            boto_config_kwargs["s3"] = {"addressing_style": "path"}
        if connect_timeout is not None or read_timeout is not None:
            boto_config_kwargs["connect_timeout"] = connect_timeout or 60
            boto_config_kwargs["read_timeout"] = read_timeout or 60

        kwargs: Dict = {
            "service_name": "s3",
            "region_name": config.region,
            "aws_access_key_id": config.access_key,
            "aws_secret_access_key": config.secret_key,
        }
        if config.endpoint:
            kwargs["endpoint_url"] = config.endpoint
        if boto_config_kwargs:
            kwargs["config"] = BotoConfig(**boto_config_kwargs)

        self._client = boto3.client(**kwargs)
        self._bucket = config.bucket

    def list_objects(self, prefix: str = "") -> Iterator[Dict]:
        """Yield ``{key, size, etag}`` dicts, one page at a time."""
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "etag": obj["ETag"].strip('"'),
                }

    def verify_bucket(self) -> None:
        """Verify the S3 bucket is accessible. Raises on failure."""
        self._client.head_bucket(Bucket=self._bucket)

    def head_object(self, key: str) -> Dict:
        """Return ``{key, size, etag}`` for a single object."""
        resp = self._client.head_object(Bucket=self._bucket, Key=key)
        return {
            "key": key,
            "size": resp["ContentLength"],
            "etag": resp["ETag"].strip('"'),
        }
