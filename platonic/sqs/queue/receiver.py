import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterator, Optional, List

from mypy_boto3_sqs.type_defs import (
    MessageTypeDef,
    ReceiveMessageResultTypeDef,
)

from platonic.queue import MessageReceiveTimeout, Receiver
from platonic.timeout import InfiniteTimeout
from platonic.timeout.base import BaseTimeout, BaseTimer

from platonic.sqs.queue.errors import SQSMessageDoesNotExist
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.sqs import (
    MAX_NUMBER_OF_MESSAGES, SQSMixin,
    MAX_WAIT_TIME_SECONDS,
)
from platonic.sqs.queue.types import InternalType, ValueType


@dataclass
class SQSReceiver(SQSMixin, Receiver[ValueType]):   # noqa: WPS214
    """Queue to read stuff from."""

    timeout: BaseTimeout = field(default_factory=InfiniteTimeout)
    max_wait_time_seconds: int = MAX_WAIT_TIME_SECONDS

    # How long to wait between attempts to fetch messages from the queue
    iteration_timeout = 3  # Seconds

    @property
    def timeout_seconds(self) -> Optional[int]:
        """Convert timeout to seconds or to None."""
        if isinstance(self.timeout, Infinity):
            return None

        return int(self.timeout.total_seconds())

    def _wait_time_seconds(self, timer: BaseTimer) -> int:
        """Based on timer instance calculate SQS WaitTimeSeconds parameter."""
        return int(min(
            # The value can be no higher than 20 seconds
            float(self.max_wait_time_seconds),

            # But if the remaining allowed time is positive and
            # less than 20, we use that value as timeout to make sure
            # we do not exceed the period specified by the user.
            max(
                # Here we take precaution against negative values.
                timer.remaining_seconds,
                0,
            ),
        ))

    def receive(self) -> SQSMessage[ValueType]:
        """
        Fetch one message from the queue.

        If the queue is empty, by default block forever until a message arrives.
        See `timeout` argument of `SQSReceiver` class to see how to change that.

        The `id` field of `Message` class is provided with `ReceiptHandle`
        property of the received message. This is a non-global identifier
        which is necessary to delete the message from the queue using
        `self.acknowledge()`.
        """
        return next(self._fetch_messages_with_timeout(messages_count=1))

    def _fetch_messages_with_timeout(
        self,
        messages_count: int,
    ) -> Iterator[SQSMessage[ValueType]]:
        """Within timeout, retrieve the requested number of messages."""
        with self.timeout.timer() as timer:
            while not timer.is_expired:
                # Calculate the timeout value for SQS Long Polling.
                try:
                    raw_messages = self._receive_messages(
                        message_count=messages_count,
                        timeout_seconds=self._wait_time_seconds(timer),
                    )['Messages']
                except KeyError:
                    # We have not received any messages. Trying again.
                    continue

                yield from map(
                    self._raw_message_to_sqs_message,
                    raw_messages,
                )
                return

        raise MessageReceiveTimeout(
            queue=self,
            timeout=0,
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
        """
        Iterate over the messages from the queue.

        If queue is empty, the iterator will, by default, block forever. See
        `SQSReceiver.timeout` argument to change that behavior.
        """
        while True:
            try:
                yield from self._fetch_messages_with_timeout(
                    messages_count=MAX_NUMBER_OF_MESSAGES,
                )
            except MessageReceiveTimeout:
                return

    def _pause_while_iterating_over_queue(self) -> None:
        """
        Wait for a while if a queue is empty.

        Just in case messages appear later.
        """
        time.sleep(self.iteration_timeout)

    def _receive_messages(
        self,
        message_count: int = 1,
        timeout_seconds: Optional[int] = None,
        **kwargs,
    ) -> ReceiveMessageResultTypeDef:
        """
        Calls SQSClient.receive_message.

        Do not override.
        """
        if 'WaitTimeSeconds' not in kwargs and timeout_seconds:
            kwargs.update({
                'WaitTimeSeconds': timeout_seconds,
            })

        return self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=message_count,
            **kwargs,
        )

    def _raw_message_to_sqs_message(
        self, raw_message: MessageTypeDef,
    ) -> SQSMessage[ValueType]:
        """Convert a raw SQS message to the proper SQSMessage instance."""
        # noinspection PyTypeChecker
        return SQSMessage(
            value=self.deserialize_value(InternalType(
                raw_message['Body'],
            )),
            receipt_handle=raw_message['ReceiptHandle'],
        )

    def _receive_with_timeout(self) -> SQSMessage[ValueType]:
        """Receive message with timeout."""
        response = self._receive_messages(
            message_count=1,
            timeout_seconds=self.timeout_seconds,
        )

        raw_messages = response.get('Messages')
        if raw_messages:
            return self._raw_message_to_sqs_message(raw_messages[0])

        raise MessageReceiveTimeout(
            queue=self,
            timeout=self.timeout_seconds,
        )

    def _receive_blocking(self) -> SQSMessage[ValueType]:
        """Fetch message from the queue in a blocking way."""
        while True:
            try:
                raw_message = self._receive_messages(
                    message_count=1,
                    timeout_seconds=None,
                )['Messages'][0]

            except KeyError:
                continue

            else:
                return self._raw_message_to_sqs_message(raw_message)
