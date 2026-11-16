import argparse

import psycopg
from psycopg.types.json import Jsonb

from ipaklink_analytics.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Switch the local App collection policy")
    parser.add_argument("mode", choices=["enable", "disable"])
    args = parser.parse_args()
    settings = Settings()
    if settings.environment != "local":
        parser.error("This command only applies to a local development environment")
    enabled = args.mode == "enable"
    with psycopg.connect(settings.postgres_dsn.get_secret_value()) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (345972151,))
        row = conn.execute(
            "SELECT rules FROM analytics_collection_policy WHERE status='active' FOR UPDATE"
        ).fetchone()
        if row is None:
            raise RuntimeError("Apply PostgreSQL migrations first")
        rules = row[0]
        if rules.get("enabled") == enabled and rules.get("anonymous_enabled") == enabled:
            return
        rules.update(enabled=enabled, anonymous_enabled=enabled)
        conn.execute("UPDATE analytics_collection_policy SET status='retired' WHERE status='active'")
        version = conn.execute("SELECT coalesce(max(policy_version),0)+1 FROM analytics_collection_policy").fetchone()[0]
        conn.execute(
            "INSERT INTO analytics_collection_policy (policy_version,rules,status,effective_at) "
            "VALUES (%s,%s,'active',now())",
            (version, Jsonb(rules)),
        )


if __name__ == "__main__":
    main()
