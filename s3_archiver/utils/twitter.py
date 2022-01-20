from typing import Any
from typing import Dict
from typing import Optional


def create_twitter_payload(message: Any) -> Optional[Dict[str, Any]]:
    """Refer to documentation in this issue: https://github.com/chusteven/kafka_producer/issues/6
    to see why we are processing data this way.
    """
    data = message.value.get("data")
    if not data:
        return None
    users = message.value.get("includes", {}).get("users")
    if not users:
        return data
    if len(users) == 1:
        return {
            **data,
            **{"user": users[0]},
        }  # This is the ideal path; one tweet, one user
    return {**data, **{"users": users}}
