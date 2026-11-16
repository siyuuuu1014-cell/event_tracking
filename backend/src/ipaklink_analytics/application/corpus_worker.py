import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4

from bson import ObjectId
from confluent_kafka import Producer
from psycopg.types.json import Jsonb
from pymongo import ReturnDocument

from ipaklink_analytics.application.corpus_review import ReviewConflict, change_review
from ipaklink_analytics.domain.corpus_review import ReviewDecision


class CorpusProblem(Exception):
    pass


def safe_text(value, limit):
    return isinstance(value,str) and 1<=len(value)<=limit and "\x00" not in value


def import_items(db,session,dataset,spec):
    entry=db.corpus_import_inputs.find_one({"_id":spec["inputRef"],"datasetId":dataset["_id"],
                                           "purposeCode":spec["purposeCode"]},session=session)
    if not entry:
        raise CorpusProblem("sourceUnavailable")
    now=datetime.now(UTC)
    accepted=0
    for item in entry["items"]:
        source,origin,target,text=(item.get(name) for name in ("sourceRef","sourceLang","targetLang","text"))
        if not safe_text(source,80) or origin not in dataset["allowedLanguages"] or target not in dataset["allowedLanguages"] or not safe_text(text,20000):
            continue
        ref=ObjectId(sha256(f'{dataset["_id"]}:{source}'.encode()).digest()[:12])
        db.corpus_items.update_one({"_id":ref},{"$setOnInsert":{
            "datasetId":dataset["_id"],"sourceRef":source,"sourceLang":origin,"targetLang":target,"text":text,
            "policyRef":dataset["policyRef"],"revision":1,"reviewStatus":"pending",
            "initialStatus":"pending","createdAt":now,"expiresAt":now+timedelta(days=dataset["retentionDays"]),
        }},upsert=True,session=session)
        accepted+=1
    return len(entry["items"]),accepted,len(entry["items"])-accepted


def erase_items(db,session,dataset,spec,job):
    now=datetime.now(UTC)
    items=list(db.corpus_items.find({"datasetId":dataset["_id"],"sourceRef":spec["sourceRef"],
                                    "reviewStatus":{"$ne":"removed"}},
                                   {"sourceLang":1,"targetLang":1,"reviewStatus":1,"initialStatus":1},session=session).limit(1001))
    if len(items)>1000:
        raise CorpusProblem("scopeTooLarge")
    for item in items:
        db.corpus_items.update_one({"_id":item["_id"]},{
            "$set":{"reviewStatus":"removed","tombstoneAt":now,"initialStatus":item.get("initialStatus",item["reviewStatus"])},
            "$unset":{"text":"","fileRef":""},"$inc":{"revision":1},
            "$push":{"reviewHistory":{"at":now,"status":"removed","reason":"sourceErased",
                                      "ticket":spec["ticketId"],"operator":job["operatorId"]}},
        },session=session)
        event_id=uuid4()
        db.analytics_mongo_outbox.insert_one({
            "_id":event_id,"topic":"analytics.corpus.status","aggregateRef":str(item["_id"]),
            "payloadSafe":{"eventId":str(event_id),"eventName":"corpus_asset_status_changed","schemaVersion":1,
                           "datasetCode":dataset["datasetCode"],"status":"removed","reasonCode":"sourceErased",
                           "languagePair":item["sourceLang"]+":"+item["targetLang"]},
            "status":"pending","attempts":0,"createdAt":now,"availableAt":now,
        },session=session)
    return len(items),len(items),0


def sync_reviews(db,session,dataset,spec,job):
    entry=db.corpus_import_inputs.find_one({"_id":spec["inputRef"],"datasetId":dataset["_id"],
                                           "purposeCode":spec["purposeCode"]},session=session)
    if not entry:
        raise CorpusProblem("sourceUnavailable")
    now=datetime.now(UTC)
    accepted=0
    for item in entry["items"]:
        proof={**item,"ticketId":spec["ticketId"]}
        try:
            decision=ReviewDecision.model_validate(proof)
        except ValueError:
            continue
        row=db.corpus_items.find_one({"_id":ObjectId(decision.itemId),"datasetId":dataset["_id"]},
            {"sourceLang":1,"targetLang":1,"reviewStatus":1,"revision":1,"reviewHistory":1,"initialStatus":1},
            session=session)
        if not row:
            continue
        try:
            change_review(db,session,row,dataset,decision,job["operatorId"],job["_id"],now)
        except ReviewConflict:
            continue
        accepted+=1
    return len(entry["items"]),accepted,len(entry["items"])-accepted


def execute_job(pg,db,job,ticket_valid,logger):
    scope={"_id":job["_id"],"leaseToken":job["leaseToken"]}
    try:
        spec=job.get("requestSpec")
        if not spec or job.get("attempts",0)>3:
            raise CorpusProblem("attemptsExhausted")
        if not ticket_valid(pg,job["operatorId"],spec["ticketId"]):
            raise CorpusProblem("permissionRevoked")
        def execute(session):
            if not db.corpus_jobs.find_one(scope,{"_id":1},session=session):
                raise CorpusProblem("leaseLost")
            dataset=db.corpus_datasets.find_one({"_id":job["datasetId"],"status":"active",
                                                "purposeCode":spec["purposeCode"]},session=session)
            if not dataset:
                raise CorpusProblem("datasetUnavailable")
            if job["type"]=="import":
                counts=import_items(db,session,dataset,spec)
            elif job["type"]=="reviewSync":
                counts=sync_reviews(db,session,dataset,spec,job)
            elif job["type"]=="erase":
                counts=erase_items(db,session,dataset,spec,job)
            else:
                raise CorpusProblem("unsupportedJob")
            db.corpus_jobs.update_one(scope,{"$set":{
                "status":"succeeded","finishedAt":datetime.now(UTC),"scannedCount":counts[0],
                "acceptedCount":counts[1],"rejectedCount":counts[2],"leaseToken":None,"leaseUntil":None,
            }},session=session)
        with db.client.start_session() as session:
            session.with_transaction(execute)
    except Exception as error:
        code=str(error) if isinstance(error,CorpusProblem) else "jobExecutionFailed"
        logger.error("corpus.jobFailed",extra={"jobRef":job["_id"],"errorCode":code,"errorType":type(error).__name__})
        db.corpus_jobs.update_one(scope,{"$set":{"status":"failed","finishedAt":datetime.now(UTC),
                                               "errorCode":code,"leaseToken":None,"leaseUntil":None}})


def outbox(settings,pg,db,logger):
    now=datetime.now(UTC)
    row=db.analytics_mongo_outbox.find_one_and_update(
        {"status":"pending","availableAt":{"$lte":now},"$or":[{"leaseUntil":None},{"leaseUntil":{"$lte":now}}]},
        {"$set":{"leaseToken":uuid4(),"leaseUntil":now+timedelta(minutes=2)},"$inc":{"attempts":1}},
        sort=[("availableAt",1),("_id",1)],return_document=ReturnDocument.AFTER,
    )
    if row is None:
        return False
    payload=row["payloadSafe"]
    scope={"_id":row["_id"],"leaseToken":row["leaseToken"]}
    try:
        if row["topic"]=="analytics.audit":
            details={k:payload[k] for k in ("datasetCode","itemId","jobRef") if k in payload}
            pg.execute(
                """INSERT INTO analytics_admin_audit(audit_id,operator_id,action,ticket_id,scope_safe,result_code,request_id,occurred_at)
                   VALUES(%s,%s,%s,%s,%s,'ok',%s,%s) ON CONFLICT (audit_id) DO NOTHING""",
                (row["_id"],payload["operatorId"],payload["action"],payload.get("ticketId"),Jsonb(details),
                 payload["requestId"],row["createdAt"]),
            )
        elif row["topic"]=="analytics.corpus.status":
            publisher=Producer({"bootstrap.servers":settings.kafka_bootstrap_servers,"enable.idempotence":True,
                                "delivery.timeout.ms":5000})
            errors=[]
            envelope={**payload,"occurredAt":row["createdAt"].isoformat(),"source":"server"}
            publisher.produce("analytics.corpus.status",key=payload["eventId"],value=json.dumps(envelope),
                              on_delivery=lambda error,_:errors.append(error))
            if publisher.flush(6) != 0 or errors != [None]:
                raise CorpusProblem("brokerUnavailable")
        else:
            raise CorpusProblem("unsupportedOutbox")
        db.analytics_mongo_outbox.update_one(scope,{"$set":{"status":"published","publishedAt":now,
                                                           "leaseToken":None,"leaseUntil":None}})
    except Exception as error:
        logger.error("corpus.outboxFailed",extra={"errorType":type(error).__name__})
        db.analytics_mongo_outbox.update_one(scope,{"$set":{
            "status":"failed" if row["attempts"]>=5 else "pending","leaseToken":None,"leaseUntil":None,
            "availableAt":now+timedelta(seconds=min(300,2**row["attempts"])),
        }})
    return True


def drain_corpus(settings,pg,db,logger,ticket_valid):
    now=datetime.now(UTC)
    job=db.corpus_jobs.find_one_and_update(
        {"$or":[{"status":"queued"},{"status":"running","leaseUntil":{"$lte":now}}]},
        {"$set":{"status":"running","leaseToken":uuid4(),"leaseUntil":now+timedelta(minutes=10)},
         "$inc":{"attempts":1}},
        sort=[("createdAt",1),("_id",1)],return_document=ReturnDocument.AFTER,
    )
    if job:
        execute_job(pg,db,job,ticket_valid,logger)
    return outbox(settings,pg,db,logger) or bool(job)
