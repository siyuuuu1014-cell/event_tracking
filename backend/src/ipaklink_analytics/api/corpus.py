from collections import Counter
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from typing import Annotated, Literal
from uuid import uuid4

from bson import ObjectId
from fastapi import APIRouter, Query, Request
from pydantic import Field
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from starlette.concurrency import run_in_threadpool

from ipaklink_analytics.api.admin_grants import encoded
from ipaklink_analytics.api.admin_login import PREFIX
from ipaklink_analytics.api.collection import RequestProblem
from ipaklink_analytics.api.config_admin import audit, require_ticket
from ipaklink_analytics.api.jobs import JobId, Key, Ticket
from ipaklink_analytics.api.reporting import Page, Size, SnapshotId, require
from ipaklink_analytics.application.corpus_review import ReviewConflict, change_review
from ipaklink_analytics.application.ingest import BUSINESS_TIMEZONE
from ipaklink_analytics.application.page_reports import date_window, meta
from ipaklink_analytics.domain.ingest import WireModel
from ipaklink_analytics.infrastructure.admin_access import (
    AccessDenied, CORPUS_JOB_READ,CORPUS_JOB_WRITE,CORPUS_REVIEW,CORPUS_SUMMARY,
)

router = APIRouter(prefix=PREFIX+"/corpus",tags=["analytics-corpus"])
Code = Annotated[str,Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,79}$")]
Pair = Annotated[str,Query(pattern=r"^[A-Za-z0-9-]{2,20}:[A-Za-z0-9-]{2,20}$")]
ItemStatus = Literal["pending","approved","rejected","removed"]
SAFE_ITEMS = {"sourceLang":1,"targetLang":1,"reviewStatus":1,"revision":1,"createdAt":1,
              "reviewHistory":1,"datasetId":1,"initialStatus":1}
MAX_SUMMARY_ITEMS = 100000


class NewJob(WireModel):
    datasetCode: Code
    datasetRevision: int = Field(ge=1,le=2147483647,strict=True)
    type: Literal["import","erase"]
    purposeCode: Code
    ticketId: Ticket
    inputRef: Code | None = None
    sourceRef: Code | None = None


class Review(WireModel):
    status: Literal["approved","rejected"]
    expectedItemRevision: int = Field(ge=1,le=2147483646,strict=True)
    reasonCode: Literal["reviewPassed","reviewFailed","policyMismatch","qualityIssue"]
    ticketId: Ticket


def failure(status):
    raise RequestProblem(status,{404:"analytics.notFound",409:"analytics.versionConflict",503:"analytics.temporarilyUnavailable"}[status])


def dataset_access(actor, dataset_code):
    if not actor.all_datasets and dataset_code not in actor.dataset_codes:
        raise AccessDenied


def bounds(start,end):
    return (datetime.combine(d,datetime.min.time(),BUSINESS_TIMEZONE).astimezone(UTC) for d in (start,end))


def item_filters(dataset_id,status,pair):
    result = {"datasetId":dataset_id}
    if status:
        result["reviewStatus"] = status
    if pair:
        result["sourceLang"],result["targetLang"] = pair.split(":")
    return result


def paged(rows,page,size):
    return {"list":rows[(page-1)*size:page*size],"total":len(rows),"page":page,"size":size}


def summarize(db,actor,code,pair,status,start,end,cutoff,page,size):
    allowed = {}
    if code is not None:
        allowed["datasetCode"] = code
    elif not actor.all_datasets:
        allowed["datasetCode"] = {"$in":list(actor.dataset_codes)}
    datasets=list(db.corpus_datasets.find({**allowed,"createdAt":{"$lte":cutoff}},{"datasetCode":1}))
    by_id={d["_id"]:d["datasetCode"] for d in datasets}
    first,last=bounds(start,end)
    filt={"datasetId":{"$in":list(by_id)},"createdAt":{"$gte":first,"$lt":last,"$lte":cutoff}}
    if pair:
        filt["sourceLang"],filt["targetLang"]=pair.split(":")
    groups=Counter()
    for index,item in enumerate(db.corpus_items.find(filt,SAFE_ITEMS).max_time_ms(4000).limit(MAX_SUMMARY_ITEMS+1)):
        if index>=MAX_SUMMARY_ITEMS:
            failure(503)
        as_of=item.get("initialStatus",item["reviewStatus"])
        for step in item.get("reviewHistory",[]):
            if step["at"]<=cutoff:
                as_of=step["status"]
        if status is None or status==as_of:
            groups[by_id[item["datasetId"]],item["sourceLang"]+":"+item["targetLang"],as_of]+=1
    latest={}
    for job in db.corpus_jobs.find({"datasetId":{"$in":list(by_id)},"createdAt":{"$lte":cutoff}},
                                   {"datasetId":1}).sort([("createdAt",DESCENDING),("_id",ASCENDING)]).max_time_ms(4000):
        latest.setdefault(by_id[job["datasetId"]],job["_id"])
    rows=[{"datasetCode":code,"languagePair":pair,"status":status,"itemCount":count,"latestJobRef":latest.get(code)}
          for (code,pair,status),count in sorted(groups.items())]
    return paged(rows,page,size)


@router.get("/summary",operation_id="IPK_ANL_18")
async def summary(request: Request,fromDate: date|None=None,toDate: date|None=None,
                  snapshotId: SnapshotId=None,datasetCode: Code|None=None,
                  languagePair: Pair|None=None,status: ItemStatus|None=None,page: Page=1,size: Size=20):
    actor=await require(request,CORPUS_SUMMARY)
    if datasetCode is not None:
        dataset_access(actor,datasetCode)
    now=datetime.now(UTC)
    try:
        start,end=date_window(fromDate,toDate,now)
    except ValueError as error:
        raise RequestProblem(400,"analytics.invalidRequest") from error
    snapshot,_=await request.app.state.reports.snapshot(snapshotId)
    data=await run_in_threadpool(summarize,request.app.state.mongo,actor,datasetCode,languagePair,status,
                                 start,end,snapshot["source_watermark"],page,size) if snapshot else paged([],page,size)
    data["meta"]=meta(snapshot,start,end,None,now)
    return encoded(200,data)


def job_details(db,actor,job_ref):
    row=db.corpus_jobs.find_one({"_id":job_ref})
    if row is None:
        failure(404)
    dataset=db.corpus_datasets.find_one({"_id":row["datasetId"]},{"datasetCode":1})
    if dataset is None:
        failure(404)
    dataset_access(actor,dataset["datasetCode"])
    return job_data(row,dataset["datasetCode"])


def job_data(row,code):
    fields=("scannedCount","acceptedCount","rejectedCount","createdAt","finishedAt","errorCode")
    return {"jobRef":row["_id"],"datasetCode":code,"type":row["type"],"status":row["status"],
            **{key:row.get(key) for key in fields}}


@router.get("/jobs/{jobRef}",operation_id="IPK_ANL_19")
async def job(request: Request,jobRef: JobId):
    actor=await require(request,CORPUS_JOB_READ)
    data=await run_in_threadpool(job_details,request.app.state.mongo,actor,jobRef)
    async with request.app.state.pg.connection() as pg:
        await audit(pg,request,actor,"corpusJobRead",{"jobRef":jobRef,"datasetCode":data["datasetCode"]})
    return encoded(200,data)


def queue(db,actor,body,key,request_id):
    dataset=db.corpus_datasets.find_one({"datasetCode":body.datasetCode,"revision":body.datasetRevision})
    if dataset is None:
        failure(404)
    if dataset["purposeCode"]!=body.purposeCode or dataset["status"]!="active":
        raise AccessDenied
    scope={"operatorId":actor.principal_id,"operationCode":"corpusJob","idempotencyKey":key}
    digest=sha256(body.model_dump_json().encode()).hexdigest()
    old=db.corpus_jobs.find_one(scope)
    if not old:
        if body.inputRef and not db.corpus_import_inputs.find_one({"_id":body.inputRef,"datasetId":dataset["_id"],
                                                                    "purposeCode":body.purposeCode},{"_id":1}):
            failure(404)
        created=datetime.now(UTC)
        created=created.replace(microsecond=created.microsecond//1000*1000)
        document={"_id":"corpus_"+uuid4().hex,"datasetId":dataset["_id"],"type":body.type,"status":"queued",
                  **scope,"requestHash":digest,"scannedCount":0,"acceptedCount":0,"rejectedCount":0,
                  "requestSpec":body.model_dump(mode="json"),"createdAt":created,"attempts":0}
        outbox={"_id":uuid4(),"topic":"analytics.audit","aggregateRef":document["_id"],
                "payloadSafe":{"action":"corpusJobQueued","operatorId":actor.principal_id,"ticketId":body.ticketId,
                               "requestId":request_id,"datasetCode":body.datasetCode,"jobRef":document["_id"]},
                "status":"pending","attempts":0,"availableAt":document["createdAt"],"createdAt":document["createdAt"]}
        try:
            with db.client.start_session() as session:
                def insert(transaction):
                    db.corpus_jobs.insert_one(document,session=transaction)
                    db.analytics_mongo_outbox.insert_one(outbox,session=transaction)
                session.with_transaction(insert)
        except DuplicateKeyError:
            old=db.corpus_jobs.find_one(scope)
            if old is None:
                raise
        else:
            old=document
    if old["requestHash"]!=digest:
        raise RequestProblem(409,"analytics.idempotencyConflict")
    return {key:value for key,value in job_data({**old,"status":"queued"},body.datasetCode).items()
            if key in {"jobRef","datasetCode","type","status","createdAt"}}


@router.post("/jobs",operation_id="IPK_ANL_39",status_code=202)
async def create_job(request: Request,body: NewJob,key: Key):
    actor=await require(request,CORPUS_JOB_WRITE)
    dataset_access(actor,body.datasetCode)
    if (body.type=="import" and (not body.inputRef or body.sourceRef)) or (
        body.type=="erase" and (not body.sourceRef or body.inputRef)):
        raise RequestProblem(400,"analytics.invalidRequest")
    async with request.app.state.pg.connection() as pg:
        await require_ticket(pg,actor,body.ticketId)
    return encoded(202,await run_in_threadpool(queue,request.app.state.mongo,actor,body,key,request.state.request_id))


def review_items(db,code,revision,status,pair,page,size):
    dataset=db.corpus_datasets.find_one({"datasetCode":code,"revision":revision})
    if not dataset:
        failure(404)
    filters=item_filters(dataset["_id"],status,pair)
    docs=list(db.corpus_items.find(filters,SAFE_ITEMS).sort([("createdAt",DESCENDING),("_id",ASCENDING)])
              .skip((page-1)*size).limit(size).max_time_ms(4000))
    return {"page":page,"size":size,"total":db.corpus_items.count_documents(filters,maxTimeMS=4000),
            "list":[{"itemId":str(row["_id"]),"datasetCode":code,"datasetRevision":revision,"status":row["reviewStatus"],
                     "languagePair":row["sourceLang"]+":"+row["targetLang"],"itemRevision":row["revision"],
                     "createdAt":row["createdAt"]} for row in docs]}


@router.get("/items",operation_id="IPK_ANL_40")
async def items(request: Request,datasetCode: Code,datasetRevision: Annotated[int,Query(ge=1,le=2147483647)],
                status: ItemStatus|None=None,languagePair: Pair|None=None,page: Page=1,size: Size=20):
    actor=await require(request,CORPUS_REVIEW)
    dataset_access(actor,datasetCode)
    data=await run_in_threadpool(review_items,request.app.state.mongo,datasetCode,datasetRevision,status,languagePair,page,size)
    return encoded(200,data)


def apply_review(db,actor,item_id,body,request_id):
    if not ObjectId.is_valid(item_id):
        failure(404)
    ref=ObjectId(item_id)
    when=datetime.now(UTC)
    def transact(session):
        row=db.corpus_items.find_one({"_id":ref},SAFE_ITEMS,session=session)
        if not row:
            failure(404)
        dataset=db.corpus_datasets.find_one({"_id":row["datasetId"]},session=session)
        if not dataset:
            failure(404)
        dataset_access(actor,dataset["datasetCode"])
        try:
            return change_review(db,session,row,dataset,body,actor.principal_id,request_id,when)
        except ReviewConflict:
            failure(409)
    with db.client.start_session() as session:
        reviewed=session.with_transaction(transact)
    return {"itemId":item_id,"status":body.status,"itemRevision":body.expectedItemRevision+1,
            "reviewedAt":reviewed.replace(microsecond=reviewed.microsecond//1000*1000)}


@router.put("/items/{itemId}/review-state",operation_id="IPK_ANL_41")
async def update_review(request: Request,itemId: JobId,body: Review):
    actor=await require(request,CORPUS_REVIEW)
    if (body.status=="approved")!=(body.reasonCode=="reviewPassed"):
        raise RequestProblem(400,"analytics.invalidRequest")
    async with request.app.state.pg.connection() as pg:
        await require_ticket(pg,actor,body.ticketId)
    return encoded(200,await run_in_threadpool(apply_review,request.app.state.mongo,actor,itemId,body,request.state.request_id))
