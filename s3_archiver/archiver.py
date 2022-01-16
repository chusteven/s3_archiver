import typing as t

import argparse
import json

from kafka import KafkaConsumer


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
        "--bootstrap-server",
        dest="bootstrap_server",
        default="localhost:9092",
        help="The bootstrap server",
    )

    return parser.parse_args()


# -----------------------------------------------------------------------------
#   Begin daemon
# -----------------------------------------------------------------------------


def main() -> None:
    args = get_cli_args()

    # Other params of interest:
    # - auto_offset_reset
    # - enable_auto_commit
    # - consumer_timeout_ms
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap_server],
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    for message in consumer:
        print(
            "%s:%d:%d: key=%s value=%s"
            % (
                message.topic,
                message.partition,
                message.offset,
                message.key,
                message.value,
            )
        )


if __name__ == "__main__":
    main()
