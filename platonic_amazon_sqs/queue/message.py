import dataclasses

from platonic.queue import Message
from platonic_amazon_sqs.queue import ValueType


@dataclasses.dataclass
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    id: str
