from dataclasses import dataclass, field
from functools import partial

import boto3
from mypy_boto3_sqs import Client as SQSClient
from platonic.const import const
from typecasts import Typecasts, casts

# Max number of SQS messages receivable by single API call.
# Also, max number of SQS messages deletable by single API call.
MAX_NUMBER_OF_MESSAGES = 10

# Message in its raw form must be shorter than this.
MAX_MESSAGE_SIZE = 262144

# Max long polling time
MAX_WAIT_TIME_SECONDS = 20


@dataclass
class SQSMixin:
    """Common fields for SQS queue classes."""

    url: str
    typecasts: Typecasts = field(default_factory=const(casts))
    internal_type: type = field(default=str)
    client: SQSClient = field(default_factory=partial(boto3.client, 'sqs'))
    batch_size: int = field(default=MAX_NUMBER_OF_MESSAGES, metadata={
        '__doc__': (
            f'Max number of SQS messages to process within one API call. '
            f'Default is {MAX_NUMBER_OF_MESSAGES}. Exceeding the max value '
            f'will cause validation errors from AWS.'
        ),
    })
