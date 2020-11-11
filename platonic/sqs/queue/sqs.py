from typing import Optional

import boto3
from mypy_boto3_sqs import Client as SQSClient
from typecasts import Typecasts, casts

from platonic.sqs.queue.errors import SQSQueueURLNotSpecified

# Max number of SQS messages receivable by single API call.
MAX_NUMBER_OF_MESSAGES = 10

# Message in its raw form must be shorter than this.
MAX_MESSAGE_SIZE = 262144


class SQSMixin:
    """Common fields for SQS queue classes."""

    url: str
    typecasts: Typecasts
    internal_type: type
    client: SQSClient

    def __init__(
        self,
        url: Optional[str] = None,
        typecasts: Optional[Typecasts] = None,
        internal_type: Optional[type] = None,
        client: Optional[SQSClient] = None,
    ):
        """SQS queue endpoint."""
        self.url = url or getattr(self, 'url', None)

        if not self.url:
            raise SQSQueueURLNotSpecified(instance=self)

        if typecasts is not None:
            self.typecasts = typecasts

        elif getattr(self, 'typecasts', None) is None:
            self.typecasts = casts

        self.internal_type = (
            internal_type or
            getattr(self, 'internal_type', None) or
            str
        )

        self.client = (
            client or
            getattr(self, 'client', None) or
            boto3.client('sqs')
        )
