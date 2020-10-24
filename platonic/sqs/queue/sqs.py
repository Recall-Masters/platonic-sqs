import dataclasses
from functools import partial

import boto3
from mypy_boto3_sqs import Client as SQSClient
from typecasts import Typecasts, casts

from platonic.const import const

# Max number of SQS messages receivable by single API call.
MAX_NUMBER_OF_MESSAGES = 10

# Message in its raw form must be shorter than this.
MAX_MESSAGE_SIZE = 262144


@dataclasses.dataclass
class SQSMixin:
    """Common fields for SQS queue classes."""

    url: str
    internal_type: type = str
    typecasts: Typecasts = dataclasses.field(default_factory=const(casts))
    client: SQSClient = dataclasses.field(
        default_factory=partial(boto3.client, 'sqs'),
    )
