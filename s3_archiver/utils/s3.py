import typing as t

import gzip
import logging
import os
import pytz
import time
import traceback

from datetime import datetime
from tempfile import NamedTemporaryFile

import boto3


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

DEFAULT_REGION = os.environ.get("DEFAULT_AWS_REGION", "us-west-2")
SESSION = boto3.session.Session()
S3_CLIENT = SESSION.client("s3", region_name=DEFAULT_REGION)

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
    messages: t.List[t.Tuple[int, str]],
    bucket_name: str,
    subpath: str,
) -> None:
    """Input is expected to be a list of tuples where the first element of the tuple
    will be used for namespacing the S3 objects.

    We persist these lines into a temporary file on disk, then GZIP them, then
    upload the entire thing to S3."""
    if not messages:
        logging.info(
            "Returning early from `upload_messages_to_s3` -- no messages to upload"
        )
        return
    last_message_offset: int = messages[-1][0]
    today_as_string = datetime.now(PST).date().isoformat()
    filepath = f"{subpath}/dt={today_as_string}/{last_message_offset}.json.gz"
    logging.info(f"About to start writing {len(messages)} messages into S3")
    traceback_message = None
    start = time.time()
    with NamedTemporaryFile(
        mode="w+b", delete=True, suffix=".txt.gz", prefix="f"
    ) as tf:
        gzf = gzip.GzipFile(mode="wb", fileobj=tf)
        for _, record in messages:
            gzf.write(record.encode("utf-8"))
        gzf.close()
        tf.seek(0)  # Hmm... still very confused why I need to do this!
        try:
            S3_CLIENT.upload_fileobj(
                tf,
                bucket_name,
                filepath,
            )
        except Exception:
            traceback_message = traceback.format_exc()
    if traceback_message:
        logging.error(
            "Ran into exception when uploading file object to S3; "
            f"traceback is {traceback_message}"
        )
        return
    end = time.time()
    logging.info(
        f"Finished writing into S3 into filepath {filepath}; took "
        f"{round(end - start, 2)} seconds"
    )
