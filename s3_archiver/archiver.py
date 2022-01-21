import typing as t

import argparse
import json
import logging
import sys
import threading
import time
import traceback

from dataclasses import dataclass
from datetime import datetime

from kafka import KafkaConsumer

from s3_archiver.utils.s3 import create_bucket_if_not_exists
from s3_archiver.utils.s3 import upload_messages_to_s3


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)

BUFFER_LOCK: threading.Lock = threading.Lock()
BYTE_SIZE_TO_FLUSH: float = 2.5e7  # 25MB
SLEEP_TIME_IN_SECONDS: int = 60


@dataclass
class UploadingKillswitch:
    should_kill: bool


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


def listen_and_maybe_upload(
    buffer: t.List[t.Tuple[int, str]],
    bucket_name: str,
    s3_subpath: str,
    uploading_killswitch: UploadingKillswitch,
) -> None:
    """Polls in 1m intervals and checks whether (a) it's been 15 minutes since
    the last flush to S3 or (b) if the buffer has gotten too big [25MB]. If
    either condition is met, then flushes to S3."""
    while True:
        if uploading_killswitch.should_kill:
            break
        current_minute = datetime.now().minute
        with BUFFER_LOCK:
            if current_minute % 15 == 0 or sys.getsizeof(buffer) >= BYTE_SIZE_TO_FLUSH:
                upload_messages_to_s3(buffer, bucket_name, s3_subpath)
                buffer.clear()
            else:
                logging.info("No need to flush buffer, sleeping...")
        time.sleep(SLEEP_TIME_IN_SECONDS)


def start_uploader_daemon(
    buffer: t.List[t.Tuple[int, str]],
    bucket_name: str,
    s3_subpath: str,
    uploading_killswitch: UploadingKillswitch,
) -> threading.Thread:
    logging.info("Starting uploader daemon")
    t = threading.Thread(
        target=listen_and_maybe_upload,
        args=(
            buffer,
            bucket_name,
            s3_subpath,
            uploading_killswitch,
        ),
    )
    t.start()
    return t


# -----------------------------------------------------------------------------
#   Consuming daemon
# -----------------------------------------------------------------------------


def consume_messages(
    kafka_topic: str,
    kafka_bootstrap_server: str,
    consumer_group_name: t.Optional[str],
    buffer: t.List[t.Tuple[int, str]],
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
        data = message.value
        if data:
            with BUFFER_LOCK:
                buffer.append((message.offset, data))


# -----------------------------------------------------------------------------
#   Entrypoint
# -----------------------------------------------------------------------------


def main() -> None:
    args = get_cli_args()
    s3_bucket_name = args.s3_bucket
    create_bucket_if_not_exists(s3_bucket_name)

    message_buffer = []
    s3_subpath = args.s3_subpath
    uploading_killswitch = UploadingKillswitch(False)
    thread = start_uploader_daemon(
        buffer=message_buffer,
        bucket_name=s3_bucket_name,
        s3_subpath=s3_subpath,
        uploading_killswitch=uploading_killswitch,
    )
    try:
        consume_messages(
            kafka_topic=args.topic,
            kafka_bootstrap_server=args.bootstrap_server,
            consumer_group_name=args.consumer_group_name,
            buffer=message_buffer,
        )
    except Exception as e:
        logging.error(
            f"Ran into some exception {str(e)} with traceback {traceback.format_exc()} "
            "during consumption; stopping uploader daemon"
        )
        try:
            uploading_killswitch.should_kill = True
            thread.join()
        except Exception as te:
            logging.error(
                f"Ran into some exception {str(te)} with traceback {traceback.format_exc()} "
                "while joining to uploader daemon thread; oh wells"
            )
            pass


if __name__ == "__main__":
    main()
