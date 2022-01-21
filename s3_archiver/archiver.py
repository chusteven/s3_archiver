import typing as t

import argparse
import json
import logging
import sys
import threading

from copy import copy
from datetime import datetime

from kafka import KafkaConsumer

from s3_archiver.utils.s3 import create_bucket_if_not_exists
from s3_archiver.utils.s3 import upload_messages_to_s3


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

BYTE_SIZE_TO_FLUSH: float = 2.5e7  # 25MB


# -----------------------------------------------------------------------------
#   Parse CLI args
# -----------------------------------------------------------------------------


def get_cli_args() -> t.Any:
    parser = argparse.ArgumentParser(description="Process some integers.")

    parser.add_argument(
        "--topic",
        dest="topic",
        default=None,
        help="The Kafka topic to which we will publish messages",
    )

    parser.add_argument(
        "--s3-bucket",
        dest="s3_bucket",
        default=None,
        help="The S3 bucket into which we will write our data",
    )

    parser.add_argument(
        "--s3-subpath",
        dest="s3_subpath",
        default=None,
        help="The S3 subpath where we will write data",
    )

    parser.add_argument(
        "--consumer-group-name",
        dest="consumer_group_name",
        default=None,
        help="The consumer group name -- must be unique so offsets can be tracked",
    )

    parser.add_argument(
        "--bootstrap-server",
        dest="bootstrap_server",
        default="localhost:9092",
        help="The bootstrap server",
    )

    return parser.parse_args()


# -----------------------------------------------------------------------------
#   Uploading daemon
# -----------------------------------------------------------------------------


def maybe_upload_messages_to_s3(
    buffer: t.List[t.Tuple[int, str]], bucket_name: str, s3_subpath: str
) -> None:
    current_minute = datetime.now().minute
    if current_minute % 15 == 0 or sys.getsizeof(buffer) >= BYTE_SIZE_TO_FLUSH:
        logging.info("Time to flush buffer!")
        buffer_copy = copy(buffer)
        t = threading.Thread(
            target=upload_messages_to_s3,
            args=(
                buffer_copy,
                bucket_name,
                s3_subpath,
            ),
        )
        t.start()
        buffer.clear()


# -----------------------------------------------------------------------------
#   Consuming daemon
# -----------------------------------------------------------------------------


def consume_messages(
    kafka_topic: str,
    kafka_bootstrap_server: str,
    consumer_group_name: t.Optional[str],
    buffer: t.List[t.Tuple[int, str]],
    bucket_name: str,
    s3_subpath: str,
) -> None:
    consumer = KafkaConsumer(
        # Other params of interest:
        # - auto_offset_reset
        # - enable_auto_commit
        # - consumer_timeout_ms
        kafka_topic,
        bootstrap_servers=[kafka_bootstrap_server],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        group_id=consumer_group_name,
    )
    logging.info("Starting to consume from Kafka")
    for message in consumer:
        data = message.value.get("data")
        if data:
            buffer.append((message.offset, data))
            maybe_upload_messages_to_s3(buffer, bucket_name, s3_subpath)


# -----------------------------------------------------------------------------
#   Entrypoint
# -----------------------------------------------------------------------------


def main() -> None:
    args = get_cli_args()
    s3_bucket_name = args.s3_bucket
    create_bucket_if_not_exists(s3_bucket_name)

    consume_messages(
        kafka_topic=args.topic,
        kafka_bootstrap_server=args.bootstrap_server,
        consumer_group_name=args.consumer_group_name,
        buffer=[],
        bucket_name=s3_bucket_name,
        s3_subpath=args.s3_subpath,
    )


if __name__ == "__main__":
    main()
