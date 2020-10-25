import dataclasses

from platonic.queue import Message
from platonic.sqs.queue.types import ValueType


@dataclasses.dataclass
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    receipt_handle: str
