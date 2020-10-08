import dataclasses
import uuid
from contextlib import contextmanager
from functools import partial
from typing import TypeVar, NewType, Iterable, Iterator
from boltons.iterutils import chunked_iter

import boto3
from mypy_boto3_sqs import Client as SQSClient
from mypy_boto3_sqs.type_defs import (
    ReceiveMessageResultTypeDef,
    MessageTypeDef, SendMessageBatchRequestEntryTypeDef,
)
from typecasts import Typecasts, casts

from platonic import (
    InputQueue, Message, const, OutputQueue, QueueDoesNotExist,
    MessageDoesNotExist,
)

ValueType = TypeVar('ValueType')
InternalType = NewType('InternalType', str)


MAX_NUMBER_OF_MESSAGES = 10
"""Max number of SQS messages receivable by single API call."""


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

    def _receive_messages(
        self,
        message_count: int = 1,
    ) -> ReceiveMessageResultTypeDef:
        """
        Calls SQSClient.receive_message.

        Do not override.
        """
        return self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=message_count,
        )

    def _raw_message_to_sqs_message(
        self, raw_message: MessageTypeDef,
    ) -> SQSMessage[ValueType]:
        """Convert a raw SQS message to the proper SQSMessage instance."""
        return SQSMessage(
            value=self.deserialize_value(InternalType(
                raw_message['Body'],
            )),
            id=raw_message['ReceiptHandle'],
        )

    def receive(self) -> SQSMessage[ValueType]:
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
                raw_message, = self._receive_messages(
                    message_count=1,
                )['Messages']

                return self._raw_message_to_sqs_message(raw_message)

            except KeyError:
                continue

    def __iter__(self) -> Iterator[SQSMessage[ValueType]]:
        while True:
            try:
                raw_messages = self._receive_messages(
                    message_count=MAX_NUMBER_OF_MESSAGES,
                )['Messages']

            except KeyError:
                continue

            else:
                yield from map(
                    self._raw_message_to_sqs_message,
                    raw_messages,
                )

    def acknowledge(
        self,
        message: SQSMessage[ValueType],
    ) -> SQSMessage[ValueType]:
        """
        Acknowledge that the given message was successfully processed.

        Delete message from the queue.
        """
        try:
            self.client.delete_message(
                QueueUrl=self.url,
                ReceiptHandle=message.id,
            )

            return message

        except self.client.exceptions.ReceiptHandleIsInvalid as err:
            raise SQSMessageDoesNotExist(message=message, queue=self) from err

    @contextmanager
    def acknowledgement(self, message: SQSMessage[ValueType]):
        try:
            yield message

        finally:
            self.acknowledge(message)


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

    def send(self, instance: ValueType) -> SQSMessage[ValueType]:
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
            # FIXME this probably is not correct. `id` contains MessageId in
            #   one cases and ResponseHandle in others. Inconsistent.
            id=sqs_response['MessageId'],
        )

    def _generate_send_batch_entry(
        self,
        instance: ValueType,
    ) -> SendMessageBatchRequestEntryTypeDef:
        """Compose the entry for send_message_batch() operation."""
        return SendMessageBatchRequestEntryTypeDef(
            Id=uuid.uuid4().hex,
            MessageBody=self.serialize_value(instance),
        )

    def send_many(self, iterable: Iterable[ValueType]) -> None:
        """Send multiple messages."""
        # Per one API call, we can send no more than MAX_NUMBER_OF_MESSAGES
        # individual messages.
        batches = chunked_iter(iterable, MAX_NUMBER_OF_MESSAGES)

        for batch in batches:
            self.client.send_message_batch(
                QueueUrl=self.url,
                Entries=list(map(
                    self._generate_send_batch_entry,
                    batch,
                ))
            )
