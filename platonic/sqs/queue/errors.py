from platonic.queue import QueueDoesNotExist, MessageDoesNotExist
from platonic.sqs.queue.types import ValueType


class SQSQueueDoesNotExist(QueueDoesNotExist[ValueType]):
    """SQS Queue at {self.queue.url} does not exist."""


class SQSMessageDoesNotExist(MessageDoesNotExist[ValueType]):
    """
    There is no such message in this SQS queue.

        Message: {self.message.id}
        Queue URL: {self.queue.url}
    """
