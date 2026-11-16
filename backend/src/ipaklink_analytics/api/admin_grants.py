from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Annotated, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, Query, Request
from fastapi.encoders import jsonable_encoder
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.collection import RequestProblem, reply
from ipaklink_analytics.api.config_admin import audit, require_ticket
from ipaklink_analytics.api.reporting import Page, Size, require
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.infrastructure.admin_access import AUDIT_READ, GRANT_READ, GRANT_WRITE, AccessDenied

router = APIRouter(prefix=PREFIX, tags=["admin-access"])
Ticket = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")]
Region = Annotated[int, Field(strict=True, ge=1, le=4294967295)]
Dataset = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")]
GrantStatus = Literal["active", "expired", "revoked"]
STATUS = """CASE WHEN g.revoked_at IS NOT NULL THEN 'revoked'
                 WHEN g.expires_at<=now() THEN 'expired' ELSE 'active' END"""


class GrantDraft(BaseModel):
    model_config = ConfigDict(extra="forbid")
    principalId: str = Field(min_length=1, max_length=128)
    roleCode: str = Field(min_length=1, max_length=40)
    regionIds: list[Region] = Field(max_length=100)
    datasetCodes: list[Dataset] = Field(max_length=100)
    ticketId: Ticket
    expiresAt: AwareDatetime | None = None


class Revocation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: Literal["revoked"]
    ticketId: Ticket


def encoded(code, row):
    return reply(code, "ok", jsonable_encoder(row), {"Cache-Control": "private, no-store"})


async def pagination(cursor, select, filters, params, order, page, size):
    where = " AND ".join(filters) or "TRUE"
    await cursor.execute(
        f"SELECT count(*) AS total FROM ({select} WHERE {where}) records", params,
    )
    total = (await cursor.fetchone())["total"]
    await cursor.execute(
        f"{select} WHERE {where} ORDER BY {order} LIMIT %s OFFSET %s",
        (*params, size, (page - 1) * size),
    )
    return {"list": await cursor.fetchall(), "page": page, "size": size, "total": total}


@router.get("/access-grants", operation_id="IPK_ANL_35")
async def grants(
    request: Request,
    principalId: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    roleCode: Annotated[str | None, Query(min_length=1, max_length=40)] = None,
    status: GrantStatus | None = None,
    page: Page = 1,
    size: Size = 20,
):
    await require(request, GRANT_READ)
    clauses = [("g.principal_id=%s", principalId), ("g.role_code=%s", roleCode), (f"({STATUS})=%s", status)]
    filters, params = [], []
    for clause, value in clauses:
        if value is not None:
            filters.append(clause)
            params.append(value)
    select = f"""SELECT g.grant_id AS "grantId",g.principal_id AS "principalId",g.role_code AS "roleCode",
                        COALESCE(g.scope->'regionIds','[]'::jsonb) AS "regionIds",
                        COALESCE(g.scope->'datasetCodes','[]'::jsonb) AS "datasetCodes",
                        {STATUS} AS status,g.expires_at AS "expiresAt"
                 FROM analytics_access_grant g JOIN analytics_admin_account a ON a.account_id=g.principal_id"""
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        data = await pagination(cursor, select, tuple(filters), tuple(params), 'g.grant_id', page, size)
    return encoded(200, data)


@router.post("/access-grants", operation_id="IPK_ANL_36", status_code=201)
async def create_grant(
    request: Request, body: GrantDraft,
    key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=128,
                               pattern=r"^[!-~]+$")],
):
    actor = await require(request, GRANT_WRITE)
    regions, datasets = sorted(set(body.regionIds)), sorted(set(body.datasetCodes))
    if not regions and not datasets:
        raise RequestProblem(400, "analytics.invalidRequest")
    expires_at = body.expiresAt.astimezone(UTC) if body.expiresAt else None
    normalized = body.model_copy(update={"regionIds": regions, "datasetCodes": datasets, "expiresAt": expires_at})
    digest = sha256(normalized.model_dump_json().encode()).hexdigest()
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (f"grant:{actor.principal_id}:{key}",))
        await require_ticket(pg, actor, body.ticketId)
        await cursor.execute(
            """SELECT r.request_hash,g.grant_id AS "grantId",g.created_at AS "createdAt"
               FROM analytics_access_grant_request r JOIN analytics_access_grant g USING(grant_id)
               WHERE r.operator_id=%s AND r.idempotency_key=%s""", (actor.principal_id, key),
        )
        existing = await cursor.fetchone()
        if existing:
            if existing.pop("request_hash") != digest:
                raise RequestProblem(409, "analytics.idempotencyConflict")
            return encoded(201, {**existing, "status": "active"})
        if expires_at is not None and expires_at <= datetime.now(UTC):
            raise RequestProblem(400, "analytics.invalidRequest")
        await cursor.execute(
            "SELECT role_code FROM analytics_role WHERE role_code=%s AND status='active'", (body.roleCode,),
        )
        if await cursor.fetchone() is None:
            raise RequestProblem(404, "analytics.notFound")
        if body.roleCode not in {"operator", "analyst"} or body.principalId == actor.principal_id:
            raise AccessDenied
        await cursor.execute(
            "SELECT account_id FROM analytics_admin_account WHERE account_id=%s AND status='active' FOR SHARE",
            (body.principalId,),
        )
        if await cursor.fetchone() is None:
            raise RequestProblem(404, "analytics.notFound")
        grant_id = uuid4()
        scope = Jsonb({"regionIds": regions, "datasetCodes": datasets, "authorizationKind": "role"})
        await cursor.execute(
            """INSERT INTO analytics_access_grant(grant_id,principal_id,role_code,scope,ticket_id,expires_at)
               VALUES(%s,%s,%s,%s,%s,%s) RETURNING created_at AS "createdAt" """,
            (grant_id, body.principalId, body.roleCode, scope, body.ticketId, expires_at),
        )
        created = await cursor.fetchone()
        await pg.execute(
            """INSERT INTO analytics_access_grant_request(operator_id,idempotency_key,request_hash,grant_id)
               VALUES(%s,%s,%s,%s)""", (actor.principal_id, key, digest, grant_id),
        )
        await audit(pg, request, actor, "grantCreated", {"grantId": str(grant_id)}, body.ticketId)
    return encoded(201, {"grantId": grant_id, "status": "active", **created})


@router.put("/access-grants/{grantId}/status", operation_id="IPK_ANL_37")
async def revoke_grant(request: Request, grantId: UUID, body: Revocation):
    actor = await require(request, GRANT_WRITE)
    async with request.app.state.pg.connection() as pg:
        await require_ticket(pg, actor, body.ticketId)
        record = await (await pg.execute(
            """SELECT g.role_code,g.revoked_at FROM analytics_access_grant g
               JOIN analytics_admin_account a ON a.account_id=g.principal_id
               WHERE g.grant_id=%s FOR UPDATE OF g""", (grantId,),
        )).fetchone()
        if record is None:
            raise RequestProblem(404, "analytics.notFound")
        if record[0] not in {"operator", "analyst"}:
            raise AccessDenied
        revoked_at = record[1]
        if revoked_at is None:
            revoked_at = (await (await pg.execute(
                "UPDATE analytics_access_grant SET revoked_at=now() WHERE grant_id=%s RETURNING revoked_at",
                (grantId,),
            )).fetchone())[0]
            await audit(pg, request, actor, "grantRevoked", {"grantId": str(grantId)}, body.ticketId)
    return encoded(200, {"grantId": grantId, "status": "revoked", "revokedAt": revoked_at})


@router.get("/audit-records", operation_id="IPK_ANL_38")
async def audit_records(
    request: Request,
    ticketId: Annotated[str | None, Query(min_length=1, max_length=80)] = None,
    action: Annotated[str | None, Query(min_length=1, max_length=80)] = None,
    operatorId: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    fromDate: date | None = None,
    toDate: date | None = None,
    page: Page = 1,
    size: Size = 20,
):
    await require(request, AUDIT_READ)
    if (fromDate is None) != (toDate is None):
        raise RequestProblem(400, "analytics.invalidRequest")
    if fromDate and not timedelta(0) < toDate - fromDate <= timedelta(days=31):
        raise RequestProblem(400, "analytics.invalidRequest")
    clauses = [("ticket_id=%s", ticketId), ("action=%s", action), ("operator_id=%s", operatorId)]
    if fromDate:
        clauses.extend([
            ("occurred_at>=%s", datetime.combine(fromDate, datetime.min.time(), BUSINESS_TIMEZONE)),
            ("occurred_at<%s", datetime.combine(toDate, datetime.min.time(), BUSINESS_TIMEZONE)),
        ])
    filters, params = [], []
    for clause, value in clauses:
        if value is not None:
            filters.append(clause)
            params.append(value)
    select = """SELECT audit_id AS "auditId",operator_id AS "operatorId",action,ticket_id AS "ticketId",
                       result_code AS "resultCode",request_id AS "requestId",occurred_at AS "occurredAt"
                FROM analytics_admin_audit"""
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        data = await pagination(cursor, select, filters, params, "occurred_at DESC,audit_id DESC", page, size)
    return encoded(200, data)
