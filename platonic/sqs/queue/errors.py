import typing
from dataclasses import dataclass

from documented import DocumentedError

from platonic.queue import MessageDoesNotExist, QueueDoesNotExist

if typing.TYPE_CHECKING:  # pragma: no cover
    from platonic.sqs.queue.sqs import SQSMixin   # noqa: WPS433


class SQSQueueDoesNotExist(QueueDoesNotExist):
    """SQS Queue at {self.queue.url} does not exist."""


class SQSMessageDoesNotExist(MessageDoesNotExist):
    """
    There is no such message in this SQS queue.

        Message: {self.message.id}
        Queue URL: {self.queue.url}
    """


@dataclass
class SQSQueueURLNotSpecified(DocumentedError):
    """
    SQS Queue URL is not provided.

    {self.class_name} instance interfaces with AWS SQS queue and, for that,
    needs the queue's URL, which usually looks like

        https://sqs.us-west-2.amazonaws.com/123456789012/queue-name

    and can be specified when configuring the class:

        class {self.class_name}(...):
            ...
            url = '...'

    or when instantiating it:

        {self.class_name}(url='...', ...)
    """

    instance: 'SQSMixin'

    @property
    def class_name(self):
        """Format readable class name."""
        return self.instance.__class__.__name__
