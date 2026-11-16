import hashlib
from importlib.resources import files

import clickhouse_connect
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.postgres.migrate import migrate as migrate_postgres
from ipaklink_analytics.infrastructure.schema.mongo import create_mongo_schema


def migrate_clickhouse(settings: Settings) -> None:
    resources = files("ipaklink_analytics.infrastructure.schema").joinpath("clickhouse")
    scripts = sorted(
        (item for item in resources.iterdir() if item.name.endswith(".sql")),
        key=lambda item: item.name,
    )
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_database,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password.get_secret_value(),
        connect_timeout=5,
        send_receive_timeout=30,
    )
    try:
        client.command(
            """CREATE TABLE IF NOT EXISTS analytics_warehouse_migration (
                   migration_id String,
                   checksum FixedString(64),
                   applied_at DateTime('UTC') DEFAULT now()
               ) ENGINE = MergeTree ORDER BY migration_id"""
        )
        for script in scripts:
            text = script.read_bytes()
            checksum = hashlib.sha256(text).hexdigest()
            applied, matching = client.query(
                "SELECT count(), countIf(checksum = {checksum:String}) FROM analytics_warehouse_migration "
                "WHERE migration_id = {migrationId:String}",
                parameters={"migrationId": script.name, "checksum": checksum},
            ).result_rows[0]
            if applied:
                if matching != applied:
                    raise RuntimeError(f"Applied ClickHouse migration changed: {script.name}")
                continue

            # Migration scripts contain plain DDL without embedded semicolons.
            for statement in text.decode("utf-8").split(";"):
                if statement.strip():
                    client.command(statement)
            client.insert(
                "analytics_warehouse_migration",
                [(script.name, checksum)],
                column_names=["migration_id", "checksum"],
            )
    finally:
        client.close()


def main() -> None:
    settings = Settings()
    migrate_postgres(settings.postgres_dsn.get_secret_value())
    print("PostgreSQL schema ready", flush=True)
    migrate_clickhouse(settings)
    print("ClickHouse schema ready", flush=True)
    with MongoClient(
        settings.mongo_uri.get_secret_value(), uuidRepresentation="standard",
        serverSelectionTimeoutMS=10000,
    ) as client:
        db = client.get_database(settings.mongo_database, write_concern=WriteConcern("majority"))
        create_mongo_schema(db)
    print("MongoDB collections ready", flush=True)


if __name__ == "__main__":
    main()
