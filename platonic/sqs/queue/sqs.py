from functools import partial
from typing import Optional

import attr
import boto3
from mypy_boto3_sqs import Client as SQSClient
from typecasts import Typecasts, casts

from platonic.sqs.queue.errors import SQSQueueURLNotSpecified

# Max number of SQS messages receivable by single API call.
MAX_NUMBER_OF_MESSAGES = 10

# Message in its raw form must be shorter than this.
MAX_MESSAGE_SIZE = 262144


@attr.s(auto_attribs=True)
class SQSMixin:
    """Common fields for SQS queue classes."""

    url: Optional[str] = attr.ib(default=None)
    internal_type: type = attr.ib(default=str)
    typecasts: Typecasts = attr.ib(default=casts)
    client: SQSClient = attr.ib(factory=partial(boto3.client, 'sqs'))

    def get_url(self) -> str:
        """Return URL of the SQS queue."""
        if self.url is None:
            raise SQSQueueURLNotSpecified(instance=self)

        return self.url
