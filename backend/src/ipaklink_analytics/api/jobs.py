from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from hmac import compare_digest
from typing import Annotated, Literal
from uuid import uuid4

from fastapi import APIRouter, Header, Query, Request
from fastapi.responses import FileResponse, StreamingResponse
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from ipaklink_analytics.api.admin_grants import encoded, pagination
from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.collection import RequestProblem
from ipaklink_analytics.api.config_admin import audit, require_ticket
from ipaklink_analytics.api.reporting import Page, Size, require
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.infrastructure import export_objects
from ipaklink_analytics.infrastructure.admin_access import AccessDenied, EXPORT_READ, EXPORT_WRITE, QUALITY_READ, REPAIR_WRITE

router = APIRouter(prefix=PREFIX, tags=["analytics-jobs"])
JobStatus = Literal["queued","running","succeeded","failed"]
JobId = Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,80}$")]
Key = Annotated[str, Header(alias="Idempotency-Key",pattern=r"^[!-~]{1,128}$")]
Ticket = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")]
Offset = Annotated[str, Field(pattern=r"^(0|[1-9][0-9]{0,18})$")]
REPORTS = {
    "overview":("/overview",{}), "pages":("/pages",{}), "features":("/features",{"feature":"search"}),
    "funnels":("/funnels/F01",{}), "retention":("/retention",{}), "quality":("/quality",{}),
    "cardImpressions":("/card-impressions",{}),
}


class Range(BaseModel):
    model_config = ConfigDict(extra="forbid")
    fromDate: date
    toDate: date


class Repair(Range):
    type: Literal["rebuild","replay"]
    reasonCode: Literal["lateData","missingData","schemaFix","reconciliation"]
    ticketId: Ticket
    dryRun: bool = Field(strict=True)
    topic: Literal["analytics.event"] | None = None
    partition: int | None = Field(default=None,ge=0,le=2147483647,strict=True)
    startOffset: Offset | None = None
    endOffsetExclusive: Offset | None = None


class Export(Range):
    snapshotId: str = Field(min_length=1,max_length=80)
    reportCode: Literal["overview","pages","features","funnels","retention","quality","cardImpressions"]
    platform: Literal["ios","android"] | None = None
    regionId: int | None = Field(default=None,ge=1,le=4294967295,strict=True)
    appVersion: str | None = Field(default=None,max_length=40)
    format: Literal["csv"]


def valid_dates(start, end, maximum):
    if not (0 < (end-start).days <= maximum):
        raise RequestProblem(400,"analytics.invalidRequest")


def timestamp_filters(column, clauses, start, end):
    if (start is None)!=(end is None):
        raise RequestProblem(400,"analytics.invalidRequest")
    if start:
        valid_dates(start,end,31)
        clauses += [
            (column+">=%s",datetime.combine(start,datetime.min.time(),BUSINESS_TIMEZONE)),
            (column+"<%s",datetime.combine(end,datetime.min.time(),BUSINESS_TIMEZONE)),
        ]
    return ([key for key,value in clauses if value is not None],
            [value for _,value in clauses if value is not None])


async def idempotent(pg, actor, operation, key, body):
    digest = sha256(body.model_dump_json().encode()).hexdigest()
    await pg.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",(f"{operation}:{actor.principal_id}:{key}",))
    row = await (await pg.execute(
        """SELECT request_hash,resource_ref FROM analytics_idempotency
           WHERE principal_id=%s AND operation_code=%s AND idempotency_key=%s""",
        (actor.principal_id,operation,key),
    )).fetchone()
    if row and row[0]!=digest:
        raise RequestProblem(409,"analytics.idempotencyConflict")
    return row[1] if row else None,digest


async def remember(pg, actor, operation, key, digest, ref, code):
    await pg.execute(
        """INSERT INTO analytics_idempotency
             (principal_id,operation_code,idempotency_key,request_hash,resource_ref,response_code,expires_at)
           VALUES(%s,%s,%s,%s,%s,%s,now()+interval '30 days')""",(actor.principal_id,operation,key,digest,ref,code),
    )


@router.post("/repair-jobs",operation_id="IPK_ANL_21")
async def create_repair(request: Request,body: Repair,key: Key):
    actor = await require(request,REPAIR_WRITE)
    valid_dates(body.fromDate,body.toDate,31)
    replay_fields = (body.topic,body.partition,body.startOffset,body.endOffsetExclusive)
    if (body.type=="rebuild" and any(v is not None for v in replay_fields)) or (
        body.type=="replay" and (None in replay_fields or not 0<int(body.endOffsetExclusive)-int(body.startOffset)<=10000
                               or int(body.endOffsetExclusive)>9223372036854775807)):
        raise RequestProblem(400,"analytics.invalidRequest")
    async with request.app.state.pg.connection() as pg:
        await require_ticket(pg,actor,body.ticketId)
        ref,digest = await idempotent(pg,actor,"repair",key,body)
        if ref:
            created = (await (await pg.execute(
                "SELECT created_at FROM analytics_repair_job WHERE job_id=%s",(ref,),
            )).fetchone())[0]
        else:
            ref = "repair_"+uuid4().hex
            created = (await (await pg.execute(
                """INSERT INTO analytics_repair_job(job_id,job_type,operator_id,idempotency_key,request_hash,scope,ticket_id,dry_run)
                   VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING created_at""",
                (ref,body.type,actor.principal_id,key,digest,Jsonb(body.model_dump(mode="json")),body.ticketId,body.dryRun),
            )).fetchone())[0]
            await remember(pg,actor,"repair",key,digest,ref,200)
            await audit(pg,request,actor,"repairQueued",{"jobId":ref},body.ticketId)
    return encoded(200,{"jobId":ref,"status":"queued","createdAt":created})


REPAIR_LIST = """SELECT job_id AS "jobId",status,job_type AS type,dry_run AS "dryRun",
                        error_code AS "errorCode",created_at AS "createdAt" FROM analytics_repair_job"""


@router.get("/repair-jobs",operation_id="IPK_ANL_32")
async def repair_history(request: Request,status: JobStatus|None=None,type: Literal["rebuild","replay"]|None=None,
                         fromDate: date|None=None,toDate: date|None=None,page: Page=1,size: Size=20):
    await require(request,QUALITY_READ)
    where,params = timestamp_filters("created_at",[("status=%s",status),("job_type=%s",type)],fromDate,toDate)
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        data = await pagination(cursor,REPAIR_LIST,where,params,"created_at DESC,job_id",page,size)
    return encoded(200,data)


@router.get("/repair-jobs/{jobId}",operation_id="IPK_ANL_22")
async def repair_details(request: Request,jobId: JobId):
    await require(request,QUALITY_READ)
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await cursor.execute(
            """SELECT job_id AS "jobId",status,job_type AS type,dry_run AS "dryRun",
                      (result->>'scannedCount')::bigint AS "scannedCount",
                      (result->>'affectedDateCount')::integer AS "affectedDateCount",
                      published_snapshot_id AS "publishedSnapshotId",error_code AS "errorCode",
                      created_at AS "createdAt",finished_at AS "finishedAt"
               FROM analytics_repair_job WHERE job_id=%s""",(jobId,),
        )
        data = await cursor.fetchone()
    if data is None:
        raise RequestProblem(404,"analytics.notFound")
    return encoded(200,data)


@router.get("/repair-jobs/{jobId}/failures",operation_id="IPK_ANL_33")
async def repair_failures(request: Request,jobId: JobId,page: Page=1,size: Size=20):
    await require(request,REPAIR_WRITE)
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        await cursor.execute("SELECT 1 FROM analytics_repair_job WHERE job_id=%s",(jobId,))
        if not await cursor.fetchone():
            raise RequestProblem(404,"analytics.notFound")
        data = await pagination(cursor,
            """SELECT topic,partition,offset_value AS "offset",error_code AS "errorCode",occurred_at AS "occurredAt"
               FROM analytics_repair_failure""",["job_id=%s"],[jobId],"failure_id",page,size)
    return encoded(200,data)


@router.get("/alert-deliveries",operation_id="IPK_ANL_42")
async def alert_deliveries(request: Request,status: Literal["pending","sent","failed"]|None=None,
                           channelCode: Annotated[str|None,Query(min_length=1,max_length=40)]=None,
                           fromDate: date|None=None,toDate: date|None=None,page: Page=1,size: Size=20):
    await require(request,QUALITY_READ)
    where,params = timestamp_filters("created_at",[("status=%s",status),("channel_code=%s",channelCode)],fromDate,toDate)
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await pg.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        data = await pagination(cursor,
            """SELECT delivery_id AS "deliveryId",alert_key AS "alertKey",channel_code AS "channelCode",status,
                      attempts,snapshot_id AS "snapshotId",fact_code AS "factCode",created_at AS "createdAt",
                      CASE WHEN status='sent' THEN sent_at END AS "sentAt"
               FROM analytics_alert_delivery""",where,params,"created_at DESC,delivery_id",page,size)
    return encoded(200,data)


@router.post("/export-jobs",operation_id="IPK_ANL_24",status_code=202)
async def create_export(request: Request,body: Export,key: Key):
    actor = await require(request,EXPORT_WRITE)
    valid_dates(body.fromDate,body.toDate,31 if body.reportCode in {"quality","funnels"} else 90)
    if body.regionId is not None and not actor.all_regions and body.regionId not in actor.region_ids:
        raise AccessDenied
    if body.reportCode=="quality" and (QUALITY_READ not in actor.permissions or body.regionId is not None):
        raise AccessDenied
    spec = body.model_dump(mode="json",exclude_none=True)
    spec["allowedRegions"] = None if actor.all_regions else sorted(actor.region_ids)
    async with request.app.state.pg.connection() as pg:
        ref,digest = await idempotent(pg,actor,"export",key,body)
        if ref:
            created = (await (await pg.execute(
                "SELECT created_at FROM analytics_export_job WHERE job_id=%s",(ref,),
            )).fetchone())[0]
        else:
            await request.app.state.reports.snapshot(body.snapshotId)
            ref = "export_"+uuid4().hex
            created = (await (await pg.execute(
                """INSERT INTO analytics_export_job(job_id,snapshot_id,requester_id,query_spec,request_id,expires_at)
                   VALUES(%s,%s,%s,%s,%s,now()+interval '1 day') RETURNING created_at""",
                (ref,body.snapshotId,actor.principal_id,Jsonb(spec),request.state.request_id),
            )).fetchone())[0]
            await remember(pg,actor,"export",key,digest,ref,202)
            await audit(pg,request,actor,"exportQueued",{"jobId":ref})
    return encoded(202,{"jobId":ref,"status":"queued","createdAt":created})


async def readable_export(request,job_id):
    actor = await require(request,EXPORT_READ)
    async with request.app.state.pg.connection() as pg, pg.cursor(row_factory=dict_row) as cursor:
        await cursor.execute("SELECT * FROM analytics_export_job WHERE job_id=%s AND requester_id=%s",
                             (job_id,actor.principal_id))
        job = await cursor.fetchone()
    if not job:
        raise RequestProblem(404,"analytics.notFound")
    spec = job["query_spec"]
    regions = spec.get("allowedRegions")
    if not actor.all_regions and (regions is None or not set(regions)<=actor.region_ids):
        raise AccessDenied
    if spec["reportCode"]=="quality" and QUALITY_READ not in actor.permissions:
        raise AccessDenied
    if job["status"]=="succeeded" and job["expires_at"]<=datetime.now(UTC):
        raise RequestProblem(410,"analytics.exportExpired")
    return job


@router.get("/export-jobs/{jobId}",operation_id="IPK_ANL_25")
async def export_details(request: Request,jobId: JobId):
    job = await readable_export(request,jobId)
    succeeded = job["status"]=="succeeded"
    url = None
    if succeeded:
        expires = int(min(job["expires_at"],datetime.now(UTC)+timedelta(minutes=5)).timestamp())
        signature = request.app.state.identity.opaque("export-download",f'{jobId}:{job["requester_id"]}:{expires}')
        url = f"{PREFIX}/export-jobs/{jobId}/file?until={expires}&signature={signature}"
    return encoded(200,{"jobId":jobId,"status":job["status"],"snapshotId":job["snapshot_id"],
                        "downloadUrl":url,"expiresAt":job["expires_at"] if succeeded else None,"errorCode":job["error_code"]})


@router.get("/export-jobs/{jobId}/file",include_in_schema=False)
async def download(request: Request,jobId: JobId,until: Annotated[int,Query(ge=1)],
                   signature: Annotated[str,Query(pattern=r"^[a-f0-9]{64}$")]):
    job = await readable_export(request,jobId)
    ref = job["artifact_ref"]
    if job["status"]!="succeeded" or not ref:
        raise RequestProblem(404,"analytics.notFound")
    if until<int(datetime.now(UTC).timestamp()):
        raise RequestProblem(410,"analytics.exportExpired")
    expected = request.app.state.identity.opaque("export-download",f'{jobId}:{job["requester_id"]}:{until}')
    if not compare_digest(signature,expected):
        raise AccessDenied
    actor = await require(request,EXPORT_READ)
    async with request.app.state.pg.connection() as pg:
        await audit(pg,request,actor,"exportDownloaded",{"jobId":jobId})
    settings = request.app.state.admin_settings
    headers = {"Cache-Control":"private, no-store"}
    if ref.startswith(f"s3:exports/{jobId}/"):
        try:
            result = await run_in_threadpool(export_objects.get,settings,ref[3:])
        except export_objects.ExportStorageError as error:
            raise RequestProblem(503,"analytics.temporarilyUnavailable") from error
        if result is None:
            raise RequestProblem(410,"analytics.exportExpired")
        body = result["Body"]
        headers.update({"Content-Disposition":f'attachment; filename="{jobId}.csv"',
                        "Content-Length":str(result["ContentLength"])})
        return StreamingResponse(body.iter_chunks(64*1024),media_type="text/csv",headers=headers,
                                 background=BackgroundTask(body.close))
    path = settings.export_dir / (jobId+".csv")
    if ref!=path.name or not path.is_file():
        raise RequestProblem(410,"analytics.exportExpired")
    return FileResponse(path,filename=path.name,media_type="text/csv",headers=headers)
