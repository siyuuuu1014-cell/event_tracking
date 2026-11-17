from dataclasses import dataclass

from psycopg_pool import AsyncConnectionPool

from ipaklink_analytics.infrastructure.identity import AuthFailure

REPORT_READ = "analytics.read"
SCHEMA_READ = "analytics.config.read"
SCHEMA_WRITE = "analytics.config.write"
QUALITY_READ = "analytics.quality.read"
SNAPSHOT_WRITE = "analytics.snapshot.write"
GRANT_READ = "analytics.grant.read"
GRANT_WRITE = "analytics.grant.write"
AUDIT_READ = "analytics.audit.read"
SESSION_READ = "analytics.session.read"
EXPORT_READ = "analytics.export.read"
EXPORT_WRITE = "analytics.export.write"
REPAIR_WRITE = "analytics.repair.write"
CORPUS_SUMMARY = "corpus.summary.read"
CORPUS_JOB_READ = "corpus.job.read"
CORPUS_JOB_WRITE = "corpus.job.write"
CORPUS_REVIEW = "corpus.item.review"
REGION_PERMISSIONS = {REPORT_READ,EXPORT_READ,EXPORT_WRITE}
DATASET_PERMISSIONS = {CORPUS_SUMMARY,CORPUS_JOB_READ,CORPUS_JOB_WRITE,CORPUS_REVIEW}
STAFF_ROLE = "analyst"
ROLE_PERMISSIONS = {
    STAFF_ROLE: REGION_PERMISSIONS | {CORPUS_SUMMARY,CORPUS_JOB_READ},
    "platformAdmin": {REPORT_READ, SCHEMA_READ, SCHEMA_WRITE, QUALITY_READ, SNAPSHOT_WRITE,
                      GRANT_READ, GRANT_WRITE, AUDIT_READ, SESSION_READ, REPAIR_WRITE} | REGION_PERMISSIONS | DATASET_PERMISSIONS,
}


class AccessDenied(Exception):
    pass


@dataclass(frozen=True)
class Actor:
    principal_id: str
    permissions: frozenset[str]
    all_regions: bool
    region_ids: frozenset[int]
    all_datasets: bool
    dataset_codes: frozenset[str]


class AdminAccess:
    def __init__(self, pool: AsyncConnectionPool) -> None:
        self.pool = pool

    async def require(self, principal: str, permission: str) -> Actor:
        async with self.pool.connection() as pg:
            records = await (await pg.execute(
                """SELECT r.role_code, g.scope FROM analytics_admin_account a
                   LEFT JOIN analytics_access_grant g ON g.principal_id=a.account_id
                     AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())
                     AND (g.ticket_id IS NULL OR g.scope->>'authorizationKind'='role')
                   LEFT JOIN analytics_role r ON r.role_code=g.role_code AND r.status='active'
                   WHERE a.account_id=%s AND a.status='active'""", (principal,),
            )).fetchall()
        if not records:
            raise AuthFailure
        permissions: set[str] = set()
        regions: set[int] = set()
        all_regions = False
        all_datasets = False
        datasets: set[str] = set()
        for code, scope in records:
            if code not in ROLE_PERMISSIONS or not isinstance(scope, dict):
                continue
            global_grant = scope.get("allRegions") is True
            role_permissions = ROLE_PERMISSIONS[code]
            values = scope.get("regionIds")
            scoped_regions = {v for v in values if type(v) is int and 0 < v <= 4294967295} if isinstance(values, list) else set()
            scope_datasets = scope.get("datasetCodes")
            scoped_datasets = {v for v in scope_datasets if isinstance(v,str) and v} if isinstance(scope_datasets,list) else set()
            effective = role_permissions if global_grant else role_permissions & (
                (REGION_PERMISSIONS if scoped_regions else set()) | (DATASET_PERMISSIONS if scoped_datasets else set())
            )
            permissions.update(effective)
            if permission in effective:
                all_regions |= global_grant
                regions.update(scoped_regions)
                all_datasets |= global_grant
                datasets.update(scoped_datasets)
        if permission not in permissions:
            raise AccessDenied
        return Actor(principal, frozenset(permissions), all_regions, frozenset(regions),all_datasets,frozenset(datasets))
