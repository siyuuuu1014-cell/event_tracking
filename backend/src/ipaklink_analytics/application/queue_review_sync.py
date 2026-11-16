import argparse
from types import SimpleNamespace
from typing import Literal
from uuid import uuid4

import psycopg
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

from ipaklink_analytics.api.corpus import Code, NewJob, queue
from ipaklink_analytics.application.job_worker import ticket_valid
from ipaklink_analytics.config import Settings


class ReviewSync(NewJob):
    type: Literal["reviewSync"] = "reviewSync"
    inputRef: Code
    sourceRef: None = None


def main():
    parser=argparse.ArgumentParser(description="Queue a staged corpus review synchronization")
    for name in ("operator","dataset","purpose","ticket","input_ref","key"):
        parser.add_argument(name)
    parser.add_argument("revision",type=int)
    options=parser.parse_args()
    settings=Settings()
    spec=ReviewSync(datasetCode=options.dataset,datasetRevision=options.revision,purposeCode=options.purpose,
                    ticketId=options.ticket,inputRef=options.input_ref)
    if not 1<=len(options.key)<=128 or any(not 33<=ord(c)<=126 for c in options.key):
        parser.error("Use a valid idempotency key")
    from psycopg.rows import dict_row
    with psycopg.connect(settings.postgres_dsn.get_secret_value(),row_factory=dict_row) as pg:
        if not ticket_valid(pg,options.operator,spec.ticketId):
            parser.error("An active platform admin and approved ticket are required")
    actor=SimpleNamespace(principal_id=options.operator)
    with MongoClient(settings.mongo_uri.get_secret_value(),uuidRepresentation="standard",tz_aware=True,
                      serverSelectionTimeoutMS=3000,timeoutMS=10000) as client:
        db=client.get_database(settings.mongo_database,write_concern=WriteConcern("majority"))
        job=queue(db,actor,spec,options.key,str(uuid4()))
    print(job["jobRef"])


if __name__=="__main__":
    main()
