import json
import re
from importlib.resources import files
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import APIRouter, Query, Request
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import Field

from ipaklink_analytics.api.collection import RequestProblem
from ipaklink_analytics.api.reporting import Page, Size, require
from ipaklink_analytics.application.build_pages import PUBLISH_LOCK
from ipaklink_analytics.domain.ingest import AppVersion, WireModel
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, QUALITY_READ, SCHEMA_READ, SCHEMA_WRITE, SNAPSHOT_WRITE

router = APIRouter(prefix="/ipaklink/v1/internal/analytics", tags=["analytics-configuration"])
CONFIG_LOCK = 345972151
_seed = files("ipaklink_analytics.infrastructure.schema").joinpath("postgres")
APPROVED_SCHEMA = {name: json.loads(schema) for name, schema in re.findall(
    r"\('([a-z_]+)', 1, 'app', \$schema\$(.*?)\$schema\$", "\n".join(_seed.joinpath(name).read_text(encoding="utf-8") for name in ("003_app_schemas.sql", "007_card_schemas.sql"))
)}


def ok(data):
    return {"code": 200, "message": "ok", "data": data}


async def list_rows(pool, base, columns, filters, order, page, size):
    conditions, values = [], []
    for column, value in filters:
        if value is not None:
            conditions.append(f"{column}=%s")
            values.append(value)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    async with pool.connection() as connection:
        async with connection.transaction(), connection.cursor(row_factory=dict_row) as cursor:
            await connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            await cursor.execute(f"SELECT count(*) AS total FROM {base}{where}", values)
            total = (await cursor.fetchone())["total"]
            await cursor.execute(
                f"SELECT {columns} FROM {base}{where} ORDER BY {order} LIMIT %s OFFSET %s",
                [*values, size, (page - 1) * size],
            )
            rows = await cursor.fetchall()
    return rows, total


def page_response(rows, total, page, size):
    return ok({"list": rows, "total": total, "page": page, "size": size})


def schema_response(row, detailed=False):
    result = {
        "eventName": row["event_name"], "schemaVersion": row["schema_version"],
        "source": row["source"], "status": row["status"], "createdAt": row["created_at"],
    }
    if detailed:
        result.update(propertySchemaText=json.dumps(row["property_schema"], ensure_ascii=False),
                      activatedAt=row["activated_at"])
    return result


def policy_response(row, detailed=False):
    rules = row["rules"]
    result = {
        "policyVersion": row["policy_version"], "status": row["status"], "enabled": rules["enabled"],
        "anonymousEnabled": rules["anonymous_enabled"], "rolloutPercent": rules.get("rollout_percent", 100),
        "platforms": rules.get("platforms", []), "effectiveAt": row["effective_at"],
    }
    if detailed:
        result.update(appVersions=rules.get("app_versions", []),
                      enabledEventNames=row["names"] if rules.get("enabled_event_names") is None
                      else rules["enabled_event_names"])
    return result


@router.get("/event-schemas", operation_id="IPK_ANL_26")
async def list_schemas(
    request: Request,
    eventName: Annotated[str | None, Query(max_length=80)] = None,
    source: Literal["app", "server"] | None = None,
    status: Literal["draft", "active", "retired"] | None = None, page: Page = 1, size: Size = 20,
):
    await require(request, SCHEMA_READ)
    rows, total = await list_rows(
        request.app.state.pg, "analytics_event_schema", "event_name,schema_version,source,status,created_at",
        [("event_name", eventName), ("source", source), ("status", status)],
        "event_name ASC,schema_version DESC", page, size,
    )
    return page_response([schema_response(row) for row in rows], total, page, size)


@router.get("/event-schemas/{eventName}/versions/{schemaVersion}", operation_id="IPK_ANL_43")
async def schema_detail(
    request: Request, eventName: str, schemaVersion: int,
):
    await require(request, SCHEMA_READ)
    if not 0 < schemaVersion <= 65535 or not eventName or len(eventName) > 80:
        raise RequestProblem(400, "analytics.invalidRequest")
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            "SELECT * FROM analytics_event_schema WHERE event_name=%s AND schema_version=%s",
            (eventName, schemaVersion),
        )
        row = await cursor.fetchone()
    if row is None:
        raise RequestProblem(404, "analytics.notFound")
    return ok(schema_response(row, detailed=True))


@router.get("/collection-policies", operation_id="IPK_ANL_29")
async def list_policies(
    request: Request,
    status: Literal["draft", "active", "retired"] | None = None, page: Page = 1, size: Size = 20,
):
    await require(request, SCHEMA_READ)
    rows, total = await list_rows(
        request.app.state.pg, "analytics_collection_policy", "policy_version,rules,status,effective_at",
        [("status", status)], "policy_version DESC", page, size,
    )
    return page_response([policy_response(row) for row in rows], total, page, size)


@router.get("/collection-policies/{policyVersion}", operation_id="IPK_ANL_45")
async def policy_detail(request: Request, policyVersion: int):
    await require(request, SCHEMA_READ)
    if not 0 < policyVersion <= 2147483647:
        raise RequestProblem(400, "analytics.invalidRequest")
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT p.*,ARRAY(SELECT DISTINCT event_name FROM analytics_event_schema
                         WHERE source='app' AND status='active' ORDER BY event_name) AS names
               FROM analytics_collection_policy p WHERE policy_version=%s""",
            (policyVersion,),
        )
        row = await cursor.fetchone()
    if row is None:
        raise RequestProblem(404, "analytics.notFound")
    return ok(policy_response(row, detailed=True))

async def audit(pg, request, actor, action, details, ticket=None):
    await pg.execute(
        """INSERT INTO analytics_admin_audit
           (audit_id,operator_id,action,ticket_id,scope_safe,result_code,request_id)
           VALUES (%s,%s,%s,%s,%s,'ok',%s)""",
        (uuid4(), actor.principal_id, action, ticket, Jsonb(details), request.state.request_id),
    )


async def require_ticket(pg, actor, ticket):
    row = await (await pg.execute(
        """SELECT 1 FROM analytics_access_grant AS g JOIN analytics_role AS r USING (role_code)
           WHERE principal_id=%s AND ticket_id=%s AND r.status='active'
             AND r.role_code='platformAdmin' AND g.scope->'allRegions'='true'::jsonb
             AND g.scope->>'authorizationKind' IS DISTINCT FROM 'role'
             AND g.revoked_at IS NULL AND (g.expires_at IS NULL OR g.expires_at>now())
           LIMIT 1""",
        (actor.principal_id, ticket),
    )).fetchone()
    if row is None:
        raise AccessDenied


class SchemaDraft(WireModel):
    eventName: str = Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=80)
    schemaVersion: int = Field(ge=1, le=65535, strict=True)
    source: Literal["app", "server"]
    propertySchemaText: str = Field(min_length=2, max_length=8192)


class SchemaStatus(WireModel):
    status: Literal["active", "retired"]
    ticketId: str = Field(min_length=1, max_length=80)


class PolicyDraft(WireModel):
    enabled: bool = Field(strict=True)
    anonymousEnabled: bool = Field(strict=True)
    rolloutPercent: int = Field(ge=0, le=100, strict=True)
    platforms: list[Literal["ios", "android"]] = Field(max_length=2)
    appVersions: list[AppVersion] = Field(max_length=100)
    enabledEventNames: list[Annotated[str, Field(pattern=r"^[a-z][a-z0-9_]*$", max_length=80)]] = Field(max_length=100)


class PolicyActivation(WireModel):
    policyVersion: int = Field(ge=1, le=2147483647, strict=True)
    expectedPolicyVersion: int = Field(ge=1, le=2147483647, strict=True)
    ticketId: str = Field(min_length=1, max_length=80)


@router.post("/event-schemas", operation_id="IPK_ANL_27", status_code=201)
async def create_schema(request: Request, body: SchemaDraft):
    actor = await require(request, SCHEMA_WRITE)
    try:
        schema = json.loads(body.propertySchemaText)
        if not isinstance(schema, dict) or schema.get("type") != "object" or schema.get("additionalProperties") is not False:
            raise ValueError
        Draft202012Validator.check_schema(schema)
    except (ValueError, RecursionError, SchemaError) as error:
        raise RequestProblem(400, "analytics.invalidRequest") from error
    async with request.app.state.pg.connection() as pg, pg.transaction(), pg.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """INSERT INTO analytics_event_schema(event_name,schema_version,source,property_schema,status)
               VALUES (%s,%s,%s,%s,'draft') ON CONFLICT (event_name,schema_version) DO NOTHING
               RETURNING event_name,schema_version,source,status,created_at""",
            (body.eventName, body.schemaVersion, body.source, Jsonb(schema)),
        )
        row = await cursor.fetchone()
        if row is None:
            raise RequestProblem(409, "analytics.versionConflict")
        await audit(pg, request, actor, "eventSchemaCreated", {"eventName":body.eventName,"schemaVersion":body.schemaVersion})
    return {"code": 201, "message": "ok", "data": schema_response(row)}


@router.put("/event-schemas/{eventName}/versions/{schemaVersion}/status", operation_id="IPK_ANL_28")
async def schema_status(
    request: Request, eventName: str, schemaVersion: int, body: SchemaStatus,
):
    actor = await require(request, SCHEMA_WRITE)
    async with request.app.state.pg.connection() as pg, pg.transaction(), pg.cursor(row_factory=dict_row) as cursor:
        await require_ticket(pg, actor, body.ticketId)
        await pg.execute("SELECT pg_advisory_xact_lock(%s)", (CONFIG_LOCK,))
        await cursor.execute(
            "SELECT * FROM analytics_event_schema WHERE event_name=%s AND schema_version=%s FOR UPDATE",
            (eventName, schemaVersion),
        )
        row = await cursor.fetchone()
        if row is None:
            raise RequestProblem(404, "analytics.notFound")
        if row["status"] == body.status:
            return ok({"eventName":eventName,"schemaVersion":schemaVersion,"status":body.status,"activatedAt":row["activated_at"]})
        if body.status == "active":
            # The applied migration is the approved App field allowlist. A draft cannot widen it.
            approved = APPROVED_SCHEMA.get(eventName)
            if row["source"] != "app" or approved is None or row["property_schema"] != approved:
                raise RequestProblem(409, "analytics.versionConflict")
        elif row["status"] != "active":
            raise RequestProblem(409, "analytics.versionConflict")
        await cursor.execute(
            """UPDATE analytics_event_schema SET status=%s,
                  activated_at=CASE WHEN %s='active' THEN COALESCE(activated_at,now()) ELSE activated_at END
               WHERE event_name=%s AND schema_version=%s RETURNING activated_at""",
            (body.status, body.status, eventName, schemaVersion),
        )
        updated = await cursor.fetchone()
        await audit(pg, request, actor, "eventSchemaStatus", {"eventName":eventName,"schemaVersion":schemaVersion,"status":body.status}, body.ticketId)
    return ok({"eventName":eventName,"schemaVersion":schemaVersion,"status":body.status,"activatedAt":updated["activated_at"]})


async def validate_names(pg, names):
    if not names:
        return
    valid = await (await pg.execute(
        """SELECT DISTINCT event_name FROM analytics_event_schema
           WHERE source='app' AND status='active' AND event_name=ANY(%s)""", (list(names),),
    )).fetchall()
    if {row[0] for row in valid} != set(names):
        raise RequestProblem(400, "analytics.invalidRequest")


@router.post("/collection-policies", operation_id="IPK_ANL_30", status_code=201)
async def create_policy(request: Request, body: PolicyDraft):
    actor = await require(request, SCHEMA_WRITE)
    async with request.app.state.pg.connection() as pg, pg.transaction():
        await pg.execute("SELECT pg_advisory_xact_lock(%s)", (CONFIG_LOCK,))
        await validate_names(pg, body.enabledEventNames)
        version = (await (await pg.execute("SELECT coalesce(max(policy_version),0)+1 FROM analytics_collection_policy")).fetchone())[0]
        rules = {
            "enabled": body.enabled, "anonymous_enabled": body.anonymousEnabled,
            "rollout_percent": body.rolloutPercent, "platforms": list(dict.fromkeys(body.platforms)),
            "app_versions": list(dict.fromkeys(body.appVersions)),
            "enabled_event_names": list(dict.fromkeys(body.enabledEventNames)),
        }
        created = (await (await pg.execute(
            """INSERT INTO analytics_collection_policy(policy_version,rules,status) VALUES (%s,%s,'draft')
               RETURNING created_at""", (version, Jsonb(rules)),
        )).fetchone())[0]
        await audit(pg, request, actor, "collectionPolicyCreated", {"policyVersion":version})
    return {"code":201,"message":"ok","data":{"policyVersion":version,"status":"draft","createdAt":created}}


@router.put("/collection-policies/current", operation_id="IPK_ANL_31")
async def activate_policy(request: Request, body: PolicyActivation):
    actor = await require(request, SCHEMA_WRITE)
    async with request.app.state.pg.connection() as pg, pg.transaction():
        await require_ticket(pg, actor, body.ticketId)
        await pg.execute("SELECT pg_advisory_xact_lock(%s)", (CONFIG_LOCK,))
        current = await (await pg.execute(
            "SELECT policy_version,effective_at FROM analytics_collection_policy WHERE status='active' FOR UPDATE"
        )).fetchone()
        target = await (await pg.execute(
            "SELECT status,rules FROM analytics_collection_policy WHERE policy_version=%s FOR UPDATE",
            (body.policyVersion,),
        )).fetchone()
        if target is None:
            raise RequestProblem(404, "analytics.notFound")
        if current and current[0] == body.policyVersion:
            replay = await (await pg.execute(
                """SELECT scope_safe->>'previousPolicyVersion' FROM analytics_admin_audit WHERE action='collectionPolicyActivated'
                   AND scope_safe->>'policyVersion'=%s ORDER BY occurred_at DESC LIMIT 1""",
                (str(body.policyVersion),),
            )).fetchone()
            previous = int(replay[0]) if replay and replay[0] is not None else None
            return ok({"policyVersion":body.policyVersion,"previousPolicyVersion":previous,"status":"active","effectiveAt":current[1]})
        if current is None or current[0] != body.expectedPolicyVersion or target[0] != "draft":
            raise RequestProblem(409, "analytics.versionConflict")
        await validate_names(pg, target[1].get("enabled_event_names"))
        await pg.execute("UPDATE analytics_collection_policy SET status='retired' WHERE policy_version=%s", (current[0],))
        effective = (await (await pg.execute(
            """UPDATE analytics_collection_policy SET status='active',effective_at=now()
               WHERE policy_version=%s RETURNING effective_at""", (body.policyVersion,),
        )).fetchone())[0]
        await audit(pg, request, actor, "collectionPolicyActivated",
                    {"policyVersion":body.policyVersion,"previousPolicyVersion":current[0]},body.ticketId)
    return ok({"policyVersion":body.policyVersion,"previousPolicyVersion":current[0],"status":"active","effectiveAt":effective})

@router.get("/report-snapshots", operation_id="IPK_ANL_44")
async def list_snapshots(
    request: Request,
    status: Literal["published", "archived", "expired"] | None = None, page: Page = 1, size: Size = 20,
):
    actor = await require(request, QUALITY_READ)
    rows, total = await list_rows(
        request.app.state.pg, "analytics_report_snapshot",
        """snapshot_id,metric_version,status,generated_at,source_watermark,
           status='archived' AND (expires_at IS NULL OR expires_at>now())
            AND EXISTS (SELECT 1 FROM analytics_report_snapshot_build AS x
                         JOIN analytics_report_build AS b USING(dataset,date_key,generation)
                         WHERE x.snapshot_id=analytics_report_snapshot.snapshot_id
                           AND b.status='verified')
            AND NOT EXISTS (SELECT 1 FROM analytics_report_snapshot_build AS x
                         JOIN analytics_report_build AS b USING(dataset,date_key,generation)
                         WHERE x.snapshot_id=analytics_report_snapshot.snapshot_id
                           AND b.status<>'verified') AS can_restore""",
        [("status", status)], "generated_at DESC,snapshot_id ASC", page, size,
    )
    restore = SNAPSHOT_WRITE in actor.permissions
    return page_response([
        {"snapshotId":r["snapshot_id"],"metricVersion":r["metric_version"],"status":r["status"],
         "generatedAt":r["generated_at"],"sourceWatermark":r["source_watermark"],
         "canRestore":restore and r["can_restore"]} for r in rows
    ], total, page, size)


class SnapshotSwitch(WireModel):
    snapshotId: str = Field(min_length=1, max_length=80)
    expectedSnapshotId: str = Field(min_length=1, max_length=80)
    ticketId: str = Field(min_length=1, max_length=80)
    reasonCode: Literal["lateData", "missingData", "schemaFix", "reconciliation"]


@router.put("/report-snapshots/current", operation_id="IPK_ANL_34")
async def switch_snapshot(request: Request, body: SnapshotSwitch):
    actor = await require(request, SNAPSHOT_WRITE)
    async with request.app.state.pg.connection() as pg, pg.transaction():
        await require_ticket(pg, actor, body.ticketId)
        await pg.execute("SELECT pg_advisory_xact_lock(%s)", (PUBLISH_LOCK,))
        current = await (await pg.execute(
            "SELECT snapshot_id FROM analytics_report_snapshot WHERE status='published' FOR UPDATE"
        )).fetchone()
        target = await (await pg.execute("""
            SELECT status, (expires_at IS NOT NULL AND expires_at<=now()) FROM analytics_report_snapshot
            WHERE snapshot_id=%s FOR UPDATE""", (body.snapshotId,)
        )).fetchone()
        if not target:
            raise RequestProblem(404, "analytics.notFound")
        if current and current[0] == body.snapshotId:
            previous = await (await pg.execute(
                """SELECT occurred_at,scope_safe->>'previousSnapshotId' FROM analytics_admin_audit
                   WHERE action='snapshotRollback' AND scope_safe->>'snapshotId'=%s
                   ORDER BY occurred_at DESC LIMIT 1""", (body.snapshotId,)
            )).fetchone()
            if previous and previous[1] == body.expectedSnapshotId:
                return ok({"snapshotId":body.snapshotId,"previousSnapshotId":previous[1],"status":"published","updatedAt":previous[0]})
        if not current or current[0] != body.expectedSnapshotId or target[0] != "archived" or target[1]:
            raise RequestProblem(409, "analytics.versionConflict")
        total, valid = (await (await pg.execute(
            """SELECT count(*),count(*) FILTER (WHERE b.status='verified')
               FROM analytics_report_snapshot_build x
               JOIN analytics_report_build b USING(dataset,date_key,generation) WHERE x.snapshot_id=%s""",
            (body.snapshotId,),
        )).fetchone())
        if total == 0 or total != valid:
            raise RequestProblem(409, "analytics.versionConflict")
        # Shutdown switch until subject deletion is reflected in snapshot invalidation.
        if (await (await pg.execute("SELECT 1 FROM analytics_subject_erasure LIMIT 1")).fetchone()) is not None:
            raise RequestProblem(409, "analytics.versionConflict")
        updated = (await (await pg.execute(
            "UPDATE analytics_report_snapshot SET status='archived',expires_at=now()+interval '7 days' "
            "WHERE snapshot_id=%s RETURNING now()", (current[0],)
        )).fetchone())[0]
        await pg.execute(
            "UPDATE analytics_report_snapshot SET status='published',expires_at=NULL WHERE snapshot_id=%s",
            (body.snapshotId,),
        )
        await audit(pg, request, actor, "snapshotRollback",
                    {"snapshotId":body.snapshotId,"previousSnapshotId":current[0],"reasonCode":body.reasonCode},body.ticketId)
    return ok({"snapshotId":body.snapshotId,"previousSnapshotId":current[0],"status":"published","updatedAt":updated})
