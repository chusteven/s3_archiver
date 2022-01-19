import typing as t

import json
import logging
import os

from datetime import datetime

import boto3


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

DEFAULT_REGION = os.environ.get("DEFAULT_AWS_REGION", "us-west-2")
S3_CLIENT = boto3.client("s3", region_name=DEFAULT_REGION)


# -----------------------------------------------------------------------------
#   Utils
# -----------------------------------------------------------------------------


def create_bucket_if_not_exists(bucket_name: str) -> None:
    logging.info(
        f"Called into `create_bucket_if_not_exists` method with arg: [{bucket_name}]"
    )
    try:
        response = S3_CLIENT.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": DEFAULT_REGION,
            },
        )
        if not response.get("ResponseMetadata", {}).get("HTTPStatusCode", None) == 200:
            raise Exception(f"Response was not OK: {response}")
    except S3_CLIENT.meta.client.exceptions.BucketAlreadyExists:
        logging.info(f"Bucket {bucket_name} already exists")


def upload_messages_to_s3(
    messages: t.List[str], bucket_name: str
) -> None:
    if not messages:
        return
    last_message_offset = messages[-1][0]
    response = S3_CLIENT.put_object(
        Body="\n".join(json.dumps(x[1]) for x in messages),
        Bucket=bucket_name,
        Key=f"dt={datetime.now().date().isoformat()}/{last_message_offset}.json",
    )
    if not response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
        logging.error(f"Response was not OK: {response}")
