import argparse
from getpass import getpass
from re import fullmatch
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from ipaklink_analytics.config import Settings
from ipaklink_analytics.infrastructure.admin_sessions import PASSWORDS


def main() -> None:
    parser = argparse.ArgumentParser(description="Maintain internal analytics accounts")
    actions = parser.add_subparsers(dest="action", required=True)
    create = actions.add_parser("create")
    create.add_argument("username")
    create.add_argument("display_name")
    create.add_argument("--platform-admin", action="store_true")
    actions.add_parser("disable").add_argument("username")
    approve = actions.add_parser("approve-ticket")
    approve.add_argument("username")
    approve.add_argument("ticket")
    args = parser.parse_args()
    username = args.username.lower()
    if not fullmatch(r"[a-z0-9][a-z0-9._-]{2,63}", username):
        parser.error("Username must have 3–64 letters, digits, dots, underscores or hyphens")
    if args.action == "create":
        name = args.display_name.strip()
        if not 1 <= len(name) <= 100:
            parser.error("Display name must contain 1–100 characters")
        password = getpass("Initial password: ")
        if not 12 <= len(password) <= 256 or password != getpass("Confirm password: "):
            parser.error("Passwords must match and contain 12–256 characters")
        password_hash = PASSWORDS.hash(password)

    with psycopg.connect(Settings().postgres_dsn.get_secret_value()) as pg:
        if args.action == "create":
            account_id = f"acct_{uuid4().hex}"
            pg.execute(
                """INSERT INTO analytics_admin_account(account_id,username,display_name,password_hash)
                   VALUES(%s,%s,%s,%s)""", (account_id, username, name, password_hash),
            )
            if args.platform_admin:
                pg.execute(
                    """INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope)
                       VALUES(%s,%s,'platformAdmin',%s)""",
                    (uuid4(), account_id, Jsonb({"allRegions": True})),
                )
            print(f"Created account {account_id}")
        elif args.action == "disable":
            row = pg.execute(
                "UPDATE analytics_admin_account SET status='disabled' WHERE username=%s RETURNING account_id",
                (username,),
            ).fetchone()
            if row is None:
                parser.error("Account not found")
            print(f"Disabled account {row[0]}")
        else:
            if not fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}", args.ticket):
                parser.error("Invalid ticket ID")
            row = pg.execute(
                """INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,ticket_id,expires_at)
                   SELECT %s,a.account_id,'platformAdmin',%s,%s,now()+interval '1 hour'
                   FROM analytics_admin_account a
                   WHERE username=%s AND status='active' AND EXISTS (
                       SELECT 1 FROM analytics_access_grant g WHERE g.principal_id=a.account_id
                       AND g.role_code='platformAdmin' AND g.ticket_id IS NULL AND g.revoked_at IS NULL
                       AND (g.expires_at IS NULL OR g.expires_at>now()) AND g.scope->'allRegions'='true'::jsonb
                   ) RETURNING grant_id""",
                (uuid4(), Jsonb({"allRegions": True}), args.ticket, username),
            ).fetchone()
            if row is None:
                parser.error("Active platform administrator required")
            print(f"Approved ticket for one hour, grant {row[0]}")


if __name__ == "__main__":
    main()
