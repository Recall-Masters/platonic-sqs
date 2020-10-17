import dataclasses
import json
import uuid
from contextlib import contextmanager
from functools import partial
from typing import TypeVar, NewType, Iterable, Iterator
from boltons.iterutils import chunked_iter

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_sqs import Client as SQSClient
from mypy_boto3_sqs.type_defs import (
    ReceiveMessageResultTypeDef,
    MessageTypeDef, SendMessageBatchRequestEntryTypeDef,
)
from typecasts import Typecasts, casts

from platonic import (
    InputQueue, Message, const, OutputQueue, QueueDoesNotExist,
    MessageDoesNotExist, MessageTooLarge,
)
from platonic.queue.errors import MessageReceiveTimeout

ValueType = TypeVar('ValueType')
InternalType = NewType('InternalType', str)


MAX_NUMBER_OF_MESSAGES = 10
"""Max number of SQS messages receivable by single API call."""


MAX_MESSAGE_SIZE = 262144
"""Message must be shorter than 262144 bytes."""


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
        **kwargs,
    ) -> ReceiveMessageResultTypeDef:
        """
        Calls SQSClient.receive_message.

        Do not override.
        """
        return self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=message_count,
            **kwargs,
        )

    def _raw_message_to_sqs_message(
        self, raw_message: MessageTypeDef,
    ) -> SQSMessage[ValueType]:
        """Convert a raw SQS message to the proper SQSMessage instance."""
        return SQSMessage(
            value=self.deserialize_value(InternalType(  # type: ignore
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

    def receive_with_timeout(self, timeout: int) -> Message[ValueType]:
        """Receive with timeout."""
        response = self._receive_messages(
            message_count=1,
            WaitTimeSeconds=timeout,
        )

        raw_message, = response.get('Messages')
        if raw_message:
            return self._raw_message_to_sqs_message(raw_message)

        else:
            raise MessageReceiveTimeout(
                queue=self,
                timeout=timeout,
            )

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

    def acknowledge(  # type: ignore
        self,
        # Liskov Substitution Principle
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
    def acknowledgement(  # type: ignore
        self,
        # Liskov substitution principle
        message: SQSMessage[ValueType],
    ):
        try:
            yield message

        finally:
            self.acknowledge(message)


class SQSQueueDoesNotExist(QueueDoesNotExist[ValueType]):
    """SQS Queue at {self.queue.url} does not exist."""


class SQSMessageDoesNotExist(MessageDoesNotExist[ValueType]):
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
        message_body = self.serialize_value(instance)

        try:
            sqs_response = self.client.send_message(
                QueueUrl=self.url,
                MessageBody=message_body,
            )

        except self.client.exceptions.QueueDoesNotExist as err:
            raise SQSQueueDoesNotExist(queue=self) from err

        except self.client.exceptions.ClientError as err:
            if self._error_code_is(err, 'InvalidParameterValue'):
                raise MessageTooLarge(
                    max_supported_size=MAX_MESSAGE_SIZE,
                    message_body=message_body,
                )

            else:
                raise

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

    def _error_code_is(self, error: ClientError, error_code: str) -> bool:
        """Check error code of a boto3 ClientError."""
        return error.response['Error']['Code'] == error_code

    def send_many(self, iterable: Iterable[ValueType]) -> None:
        """Send multiple messages."""
        # Per one API call, we can send no more than MAX_NUMBER_OF_MESSAGES
        # individual messages.
        batches = chunked_iter(iterable, MAX_NUMBER_OF_MESSAGES)

        for batch in batches:
            entries = list(map(
                self._generate_send_batch_entry,
                batch,
            ))

            try:
                self.client.send_message_batch(
                    QueueUrl=self.url,
                    Entries=entries,
                )

            except self.client.exceptions.ClientError as err:
                if self._error_code_is(err, 'BatchRequestTooLong'):
                    raise MessageTooLarge(
                        max_supported_size=MAX_MESSAGE_SIZE,
                        message_body=json.dumps(entries),
                    )

                else:
                    raise
