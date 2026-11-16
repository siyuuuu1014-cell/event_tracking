import hashlib
from importlib.resources import files

import psycopg

from ipaklink_analytics.config import Settings


def migrate(dsn: str) -> None:
    resources = files("ipaklink_analytics.infrastructure.postgres").joinpath("migrations")
    scripts = sorted((item for item in resources.iterdir() if item.name.endswith(".sql")), key=lambda item: item.name)
    with psycopg.connect(dsn, autocommit=True) as connection:
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(%s)", (345972150,))
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS analytics_schema_migration (
                    migration_id text PRIMARY KEY,
                    checksum char(64) NOT NULL,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            for script in scripts:
                source = script.read_bytes()
                checksum = hashlib.sha256(source).hexdigest()
                row = connection.execute(
                    "SELECT checksum FROM analytics_schema_migration WHERE migration_id = %s",
                    (script.name,),
                ).fetchone()
                if row is not None:
                    if row[0] != checksum:
                        raise RuntimeError(f"Applied migration changed: {script.name}")
                    continue
                connection.execute(source.decode("utf-8"))
                connection.execute(
                    "INSERT INTO analytics_schema_migration (migration_id, checksum) VALUES (%s, %s)",
                    (script.name, checksum),
                )


def main() -> None:
    migrate(Settings().postgres_dsn.get_secret_value())


if __name__ == "__main__":
    main()

