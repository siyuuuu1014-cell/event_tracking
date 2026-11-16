from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ANALYTICS_")

    environment: str = "local"
    log_dir: Path = Path("logs")
    postgres_dsn: SecretStr = SecretStr(
        "postgresql://analytics:analytics_local@127.0.0.1:55432/analytics"
    )
    redis_url: SecretStr = SecretStr("redis://127.0.0.1:56379/0")
    kafka_bootstrap_servers: str = "127.0.0.1:59092"
    clickhouse_host: str = "127.0.0.1"
    clickhouse_port: int = 58123
    clickhouse_database: str = "analytics"
    clickhouse_user: str = "analytics"
    clickhouse_password: SecretStr = SecretStr("analytics_local")
    mongo_uri: SecretStr = SecretStr(
        "mongodb://127.0.0.1:57017/?directConnection=true&replicaSet=rs0"
    )
    mongo_database: str = "analytics"
    health_timeout_seconds: int = 3
    signing_secret: SecretStr = SecretStr("")
    app_jwt_secret: SecretStr = SecretStr("")
    app_jwt_issuer: str = ""
    app_jwt_audience: str = ""
    kafka_consumer_group: str = "analytics-warehouse-v1"
    export_bucket: str = ""
    export_s3_endpoint: str = ""
    export_s3_region: str = "us-east-1"
    export_s3_access_key: SecretStr = SecretStr("")
    export_s3_secret_key: SecretStr = SecretStr("")
    export_dir: Path = Path("logs/exports")
    quality_lag_threshold: int = Field(default=1000, ge=1)
    job_api_origin: str = "http://127.0.0.1:8000"
    admin_origin: str = "http://127.0.0.1:58000"

