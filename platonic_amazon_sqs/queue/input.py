import dataclasses
from contextlib import contextmanager
from typing import Iterator

from mypy_boto3_sqs.type_defs import ReceiveMessageResultTypeDef, MessageTypeDef

from platonic import InputQueue, Message
from platonic.queue.errors import MessageReceiveTimeout
from platonic_amazon_sqs import SQSMessage
from platonic_amazon_sqs.queue.types import ValueType, InternalType
from platonic_amazon_sqs.queue.sqs import MAX_NUMBER_OF_MESSAGES, SQSMixin
from platonic_amazon_sqs.queue.errors import SQSMessageDoesNotExist


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

        raw_messages = response.get('Messages')
        if raw_messages:
            raw_message, = raw_messages
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
