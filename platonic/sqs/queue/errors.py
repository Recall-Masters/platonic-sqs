from platonic.queue import MessageDoesNotExist, QueueDoesNotExist


class SQSQueueDoesNotExist(QueueDoesNotExist):
    """SQS Queue at {self.queue.url} does not exist."""


class SQSMessageDoesNotExist(MessageDoesNotExist):
    """
    There is no such message in this SQS queue.

        Message: {self.message.id}
        Queue URL: {self.queue.url}
    """
