from functools import lru_cache
from time import monotonic, sleep

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from ipaklink_analytics.config import Settings


class ExportStorageError(Exception):
    pass


@lru_cache(maxsize=8)
def s3_client(endpoint,region,access_key,secret_key):
    kwargs = {"endpoint_url":endpoint or None,"region_name":region,
              "config":Config(connect_timeout=3,read_timeout=10,retries={"total_max_attempts":2},
                              s3={"addressing_style":"path" if endpoint else "auto"})}
    if access_key:
        kwargs.update(aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    return boto3.session.Session().client("s3",**kwargs)


def storage(settings):
    if not settings.export_bucket:
        raise ExportStorageError("Export bucket is not configured")
    return s3_client(settings.export_s3_endpoint,settings.export_s3_region,
                     settings.export_s3_access_key.get_secret_value(),settings.export_s3_secret_key.get_secret_value())


def put(settings,key,data):
    try:
        storage(settings).put_object(Bucket=settings.export_bucket,Key=key,Body=data,ContentType="text/csv")
    except (BotoCoreError,ClientError) as error:
        raise ExportStorageError from error


def get(settings,key):
    try:
        return storage(settings).get_object(Bucket=settings.export_bucket,Key=key)
    except ClientError as error:
        if error.response.get("Error",{}).get("Code") in {"404","NoSuchKey","NotFound"}:
            return None
        raise ExportStorageError from error
    except BotoCoreError as error:
        raise ExportStorageError from error


def delete(settings,key):
    try:
        storage(settings).delete_object(Bucket=settings.export_bucket,Key=key)
    except (BotoCoreError,ClientError) as error:
        raise ExportStorageError from error


def main():
    settings = Settings()
    client = storage(settings)
    deadline = monotonic()+45
    while True:
        try:
            client.create_bucket(Bucket=settings.export_bucket)
            return
        except ClientError as error:
            if error.response.get("Error",{}).get("Code") == "BucketAlreadyOwnedByYou":
                return
            raise
        except BotoCoreError:
            if monotonic()>=deadline:
                raise
            sleep(1)


if __name__=="__main__":
    main()
