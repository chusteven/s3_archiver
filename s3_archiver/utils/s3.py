import typing as t

import json
import logging
import os
import pytz

from datetime import datetime

import boto3


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

DEFAULT_REGION = os.environ.get("DEFAULT_AWS_REGION", "us-west-2")
S3_CLIENT = boto3.client("s3", region_name=DEFAULT_REGION)

PST = pytz.timezone("US/Pacific")


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
        if not response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
            raise Exception(f"Response was not OK: {response}")
    except (
        S3_CLIENT.exceptions.BucketAlreadyExists,
        S3_CLIENT.exceptions.BucketAlreadyOwnedByYou,
    ):
        logging.info(f"Bucket {bucket_name} already exists")


def upload_messages_to_s3(
    messages: t.List[t.Tuple[int, str]], bucket_name: str, subpath: str
) -> None:
    """Input is expected to be a list of tuples where the first element of the tuple
    will be used for namespacing the S3 objects. We write this as a newline-separated
    series of JSON strings."""
    if not messages:
        logging.info(
            "Returning early from `upload_messages_to_s3` -- no messages to upload"
        )
    last_message_offset: int = messages[-1][0]
    today_as_string = datetime.now(PST).date().isoformat()
    logging.info(f"About to start writing {len(messages)} messages into S3")
    filepath = f"{subpath}/dt={today_as_string}/{last_message_offset}.json"
    response = S3_CLIENT.put_object(
        Body="\n".join(json.dumps(x[1]) for x in messages),
        Bucket=bucket_name,
        Key=filepath,
    )
    if not response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200:
        logging.error(f"Response was not OK: {response}")
        return
    logging.info(f"Finished writing into S3 into filepath {filepath}")
