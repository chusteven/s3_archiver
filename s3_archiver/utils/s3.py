import typing as t

import json
import logging


# -----------------------------------------------------------------------------
#   Constants
# -----------------------------------------------------------------------------


logging.basicConfig(level=logging.INFO)


# -----------------------------------------------------------------------------
#   Utils
# -----------------------------------------------------------------------------


def create_bucket_if_not_exists(bucket_name: str) -> None:
    logging.info(
        f"Called into `create_bucket_if_not_exists` method with arg: [{bucket_name}]"
    )
    pass


def upload_messages_to_s3(messages: t.List[str]) -> None:
    formatted_messages = "\n\t".join(json.dumps(x) for x in messages)
    logging.info(
        "Called into `upload_messages_to_s3` method with messages: "
        f"\n\t{formatted_messages}"
    )
    pass
