import dataclasses

from platonic.queue import Message
from platonic.sqs.queue import ValueType


@dataclasses.dataclass
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    id: str
