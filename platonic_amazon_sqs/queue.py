import dataclasses
from functools import partial
from typing import TypeVar, NewType

import boto3
from mypy_boto3_sqs import Client as SQSClient
from typecasts import Typecasts, casts

from platonic import (
    InputQueue, Message, const, OutputQueue, QueueDoesNotExist,
    MessageDoesNotExist,
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
    client: SQSClient = dataclasses.field(
        default_factory=partial(boto3.client, 'sqs'),
    )


@dataclasses.dataclass
class SQSInputQueue(SQSMixin, InputQueue[ValueType]):
    """Queue to read stuff from."""

    def get(self) -> SQSMessage[ValueType]:
        """
        Fetch one message from the queue.

        This operation is a blocking one, and will hang until a message is
        retrieved.

        The `id` field of `Message` class is provided with `ReceiptHandle`
        property of the received message. This is a non-global identifier
        which is necessary to delete the message from the queue using
        `self.acknowledge()`.
        """
        while True:
            try:
                raw_message, = self.client.receive_message(
                    QueueUrl=self.url,
                    MaxNumberOfMessages=1,
                )['Messages']

                return SQSMessage(
                    value=self.deserialize_value(InternalType(
                        raw_message['Body'],
                    )),
                    id=raw_message['ReceiptHandle'],
                )

            except KeyError:
                continue

    def acknowledge(self, message: SQSMessage[ValueType]) -> None:
        """
        Acknowledge that the given message was successfully processed.

        Delete message from the queue.
        """
        try:
            self.client.delete_message(
                QueueUrl=self.url,
                ReceiptHandle=message.id,
            )

        except self.client.exceptions.ReceiptHandleIsInvalid as err:
            raise SQSMessageDoesNotExist(message=message, queue=self) from err


class SQSQueueDoesNotExist(QueueDoesNotExist):
    """SQS Queue at {self.queue.url} does not exist."""


class SQSMessageDoesNotExist(MessageDoesNotExist):
    """
    There is no such message in this SQS queue.

        Message: {self.message.id}
        Queue URL: {self.queue.url}
    """


@dataclasses.dataclass
class SQSOutputQueue(SQSMixin, OutputQueue[ValueType]):
    """Queue to write stuff into."""

    def put(self, instance: ValueType) -> SQSMessage[ValueType]:
        """Put a message into the queue."""
        try:
            sqs_response = self.client.send_message(
                QueueUrl=self.url,
                MessageBody=self.serialize_value(instance),
            )

        except self.client.exceptions.QueueDoesNotExist as err:
            raise SQSQueueDoesNotExist(queue=self) from err

        return SQSMessage(
            value=instance,
            id=sqs_response['MessageId'],
        )
