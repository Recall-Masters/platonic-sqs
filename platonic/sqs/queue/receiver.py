import time
from contextlib import contextmanager
from typing import Iterator

from mypy_boto3_sqs.type_defs import (
    MessageTypeDef,
    ReceiveMessageResultTypeDef,
)

from platonic.queue import Message, MessageReceiveTimeout, Receiver
from platonic.sqs.queue.errors import SQSMessageDoesNotExist
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.sqs import MAX_NUMBER_OF_MESSAGES, SQSMixin
from platonic.sqs.queue.types import InternalType, ValueType


class SQSReceiver(SQSMixin, Receiver[ValueType]):
    """Queue to read stuff from."""

    iteration_timeout = 3  # Seconds

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

            except KeyError:
                continue

            else:
                return self._raw_message_to_sqs_message(raw_message)

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

        raise MessageReceiveTimeout(
            queue=self,
            timeout=timeout,
        )

    def acknowledge(
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
                ReceiptHandle=message.receipt_handle,
            )

        except self.client.exceptions.ReceiptHandleIsInvalid as err:
            raise SQSMessageDoesNotExist(message=message, queue=self) from err

        return message

    @contextmanager
    def acknowledgement(
        self,
        # Liskov substitution principle
        message: SQSMessage[ValueType],
    ):
        """
        Acknowledgement context manager.

        Into this context manager, you can wrap any operation with a given
        Message. The context manager will automatically acknowledge the message
        when and if the code in its context completes successfully.
        """
        try:  # noqa: WPS501
            yield message

        finally:
            self.acknowledge(message)

    def __iter__(self) -> Iterator[SQSMessage[ValueType]]:
        """Iterate over the messages from the queue."""
        while True:
            try:
                raw_messages = self._receive_messages(
                    message_count=MAX_NUMBER_OF_MESSAGES,
                )['Messages']

            except KeyError:
                self._pause_while_iterating_over_queue()
                continue  # pragma: no cover

            else:
                yield from map(
                    self._raw_message_to_sqs_message,
                    raw_messages,
                )

    def _pause_while_iterating_over_queue(self) -> None:
        """
        Wait for a while if a queue is empty.

        Just in case messages appear later.
        """
        time.sleep(self.iteration_timeout)

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
            value=self.deserialize_value(InternalType(
                raw_message['Body'],
            )),
            receipt_handle=raw_message['ReceiptHandle'],
        )
