from pathlib import Path

from pydantic import SecretStr
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
    health_timeout_seconds: int = 3

