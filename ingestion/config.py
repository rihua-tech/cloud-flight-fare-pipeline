import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    api_base_url: str = os.getenv("API_BASE_URL", "")
    api_key: str = os.getenv("API_KEY", "")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    s3_bucket: str = os.getenv("S3_BUCKET", "")
    s3_prefix_bronze: str = os.getenv("S3_PREFIX_BRONZE", "bronze/flights")

settings = Settings()
