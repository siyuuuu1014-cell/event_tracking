from uuid import uuid4


class ReviewConflict(Exception):
    pass


def change_review(db,session,row,dataset,body,operator_id,request_id,when):
    ref=row["_id"]
    event_id,audit_id=uuid4(),uuid4()
    previous=row.get("reviewHistory",[])
    latest=previous[-1] if previous else {}
    new_revision=body.expectedItemRevision+1
    if (row["revision"]==new_revision and latest.get("status")==body.status and latest.get("reason")==body.reasonCode
        and latest.get("ticket")==body.ticketId and latest.get("operator")==operator_id):
        return latest["at"]
    if row["revision"]!=body.expectedItemRevision or row["reviewStatus"]=="removed" or dataset["status"]!="active":
        raise ReviewConflict
    if row["reviewStatus"]==body.status:
        raise ReviewConflict
    item_id=str(ref)
    code=dataset['datasetCode']
    step={"at":when,"status":body.status,"reason":body.reasonCode,"ticket":body.ticketId,"operator":operator_id}
    db.corpus_items.update_one({"_id":ref,"revision":row["revision"]},
        {"$inc":{"revision":1},"$set":{"reviewStatus":body.status,
                "initialStatus":row.get("initialStatus",row["reviewStatus"])},"$push":{"reviewHistory":step}},session=session)
    event={"eventId":str(event_id),"eventName":"corpus_asset_status_changed","schemaVersion":1,
           "datasetCode":code,"languagePair":row["sourceLang"]+":"+row["targetLang"],
           "status":body.status,"reasonCode":body.reasonCode}
    record={"action":"corpusReview","operatorId":operator_id,"ticketId":body.ticketId,
            "requestId":request_id,"datasetCode":code,"itemId":item_id}
    db.analytics_mongo_outbox.insert_many([
        {"_id":identifier,"topic":topic,"aggregateRef":item_id,"payloadSafe":payload,"status":"pending",
         "attempts":0,"availableAt":when,"createdAt":when}
        for identifier,topic,payload in ((event_id,"analytics.corpus.status",event),(audit_id,"analytics.audit",record))],
        session=session)
    return when
