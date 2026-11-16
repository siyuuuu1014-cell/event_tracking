from pymongo import ASCENDING, DESCENDING, IndexModel
from pymongo.database import Database
from pymongo.errors import CollectionInvalid


def field(bson_type: str | list[str], **constraints) -> dict:
    return {"bsonType": bson_type, **constraints}


TEXT = field("string", minLength=1)
DATE = field("date")
NULL_DATE = field(["date", "null"])
INT = field("int", minimum=1)
COUNT = field(["long", "int"], minimum=0)
JOB_STATUS = field("string", enum=["queued", "running", "succeeded", "failed"])


def validator(properties: dict, optional: tuple[str, ...] = (), **constraints) -> dict:
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [key for key in properties if key not in optional],
            "additionalProperties": False,
            "properties": properties,
            **constraints,
        }
    }


def ensure_collection(db: Database, name: str, schema: dict, indexes: list[IndexModel]) -> None:
    try:
        db.create_collection(name, validator=schema, validationLevel="strict")
    except CollectionInvalid:
        # Create may have succeeded before a previous deployment restarted.
        db.command("collMod", name, validator=schema, validationLevel="strict")
    if indexes:
        db[name].create_indexes(indexes)


def create_mongo_schema(db: Database) -> None:
    ensure_collection(
        db, "analytics_mongo_outbox",
        validator(
            {
                "_id": field("binData"),
                "topic": TEXT,
                "aggregateRef": TEXT,
                "payloadSafe": field("object"),
                "status": field("string", enum=["pending", "published", "failed"]),
                "attempts": field("int", minimum=0),
                "availableAt": DATE,
                "leaseToken": field(["binData", "null"]),
                "leaseUntil": NULL_DATE,
                "createdAt": DATE,
                "publishedAt": NULL_DATE,
            },
            ("leaseToken", "leaseUntil", "publishedAt"),
        ),
        [IndexModel([("status", ASCENDING), ("availableAt", ASCENDING)], name="outbox_claim")],
    )
    ensure_collection(
        db, "corpus_datasets",
        validator({
            "_id": field("objectId"),
            "datasetCode": TEXT,
            "revision": INT,
            "purposeCode": TEXT,
            "allowedLanguages": field("array", items=TEXT, uniqueItems=True),
            "policyRef": TEXT,
            "retentionDays": INT,
            "status": field("string", enum=["draft", "active", "retired"]),
            "createdAt": DATE,
        }),
        [IndexModel([("datasetCode", ASCENDING), ("revision", ASCENDING)], name="dataset_revision", unique=True)],
    )
    ensure_collection(
        db, "corpus_items",
        validator(
            {
                "_id": field("objectId"),
                "datasetId": field("objectId"),
                "sourceRef": TEXT,
                "revision": INT,
                "sourceLang": TEXT,
                "targetLang": TEXT,
                "text": TEXT,
                "fileRef": TEXT,
                "reviewStatus": field("string", enum=["pending", "approved", "rejected", "removed"]),
                "policyRef": TEXT,
                "createdAt": DATE,
                "expiresAt": DATE,
                "tombstoneAt": NULL_DATE,
                "initialStatus": TEXT,
                "reviewHistory": field("array",items=field("object")),
            },
            ("text", "fileRef", "tombstoneAt", "initialStatus", "reviewHistory"),
            anyOf=[
                {"required": ["text"]},
                {"required": ["fileRef"]},
                {"required": ["tombstoneAt"], "properties": {
                    "tombstoneAt": DATE, "reviewStatus": {"enum": ["removed"]},
                }},
            ],
        ),
        [
            IndexModel([("datasetId", ASCENDING), ("sourceRef", ASCENDING), ("revision", ASCENDING)], name="item_revision", unique=True),
            IndexModel([("datasetId", ASCENDING), ("reviewStatus", ASCENDING), ("createdAt", DESCENDING)], name="item_review"),
            IndexModel([("sourceRef", ASCENDING), ("tombstoneAt", ASCENDING)], name="item_erasure_source"),
            IndexModel([("expiresAt", ASCENDING)], name="item_due_for_erasure"),
        ],
    )
    ensure_collection(
        db, "corpus_jobs",
        validator(
            {
                "_id": TEXT,
                "datasetId": field("objectId"),
                "operatorId": TEXT,
                "operationCode": TEXT,
                "idempotencyKey": TEXT,
                "requestHash": field("string", pattern="^[a-f0-9]{64}$"),
                "type": field("string", enum=["import", "reviewSync", "erase"]),
                "status": JOB_STATUS,
                "scannedCount": COUNT,
                "acceptedCount": COUNT,
                "rejectedCount": COUNT,
                "createdAt": DATE,
                "finishedAt": NULL_DATE,
                "errorCode": field(["string", "null"]),
                "requestSpec": field("object"),
                "attempts": COUNT,
                "leaseToken": field(["binData","null"]),
                "leaseUntil": NULL_DATE,
            },
            ("finishedAt", "errorCode", "requestSpec", "attempts", "leaseToken", "leaseUntil"),
        ),
        [
            IndexModel([("datasetId", ASCENDING), ("createdAt", DESCENDING)], name="corpus_job_dataset"),
            IndexModel([("status", ASCENDING), ("createdAt", ASCENDING)], name="corpus_job_claim"),
            IndexModel(
                [("operatorId", ASCENDING), ("operationCode", ASCENDING), ("idempotencyKey", ASCENDING)],
                name="corpus_job_request", unique=True,
            ),
        ],
    )
    ensure_collection(
        db, "corpus_import_inputs",
        validator({"_id":TEXT,"datasetId":field("objectId"),"purposeCode":TEXT,
                   "items":field("array",items=field("object"),maxItems=1000),"createdAt":DATE}),
        [],
    )
