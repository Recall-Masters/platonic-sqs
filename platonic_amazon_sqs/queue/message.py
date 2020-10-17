import dataclasses

from platonic import Message
from platonic_amazon_sqs.queue.types import ValueType


@dataclasses.dataclass
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    id: str
