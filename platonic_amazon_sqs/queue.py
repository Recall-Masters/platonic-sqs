import dataclasses
from abc import ABC
from functools import cached_property
from typing import Generic, Optional, Type, TypeVar, Callable, NewType

import boto3
from mypy_boto3_sqs import Client as SQSClient
from typecasts import Typecasts, casts

from platonic import (
    InputQueue, Message, const, generic_type_args,
    OutputQueue,
)

ValueType = TypeVar('ValueType')
InternalType = NewType('InternalType', str)


@dataclasses.dataclass
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    id: str


@dataclasses.dataclass
class SQSMixin:
    """Common fields for SQS queue classes."""

    url: str
    internal_type: type = str
    typecasts: Typecasts = dataclasses.field(default_factory=const(casts))

    @cached_property
    def client(self) -> SQSClient:
        """Get AWS SQS client."""
        return boto3.client('sqs')


@dataclasses.dataclass
class SQSInputQueue(SQSMixin, InputQueue[ValueType]):
    """Queue to read stuff from."""

    def get(self) -> SQSMessage[ValueType]:
        """Fetch one message from the queue."""
        raw_message, = self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=1,
        )['Messages']

        return SQSMessage(
            value=self.deserialize_value(InternalType(raw_message['Body'])),
            id=raw_message['MessageId'],
        )

    def acknowledge(self, message: SQSMessage[ValueType]) -> None:
        """..."""
        self.client.delete_message(
            QueueUrl=self.url,
            ReceiptHandle=message.id,
        )


@dataclasses.dataclass
class SQSOutputQueue(SQSMixin, OutputQueue[ValueType]):
    """Queue to write stuff into."""

    def put(self, instance: ValueType) -> SQSMessage[ValueType]:
        """Put a message into the queue."""
        sqs_response = self.client.send_message(
            QueueUrl=self.url,
            MessageBody=self.serialize_value(instance),
        )

        return SQSMessage(
            value=instance,
            id=sqs_response['MessageId'],
        )
