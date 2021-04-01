from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Iterable, Iterator, Optional

from boltons.iterutils import chunked_iter
from mypy_boto3_sqs.type_defs import (
    MessageTypeDef,
    ReceiveMessageResultTypeDef,
)
from platonic.queue import MessageReceiveTimeout, Receiver
from platonic.sqs.queue.acknowledge import generate_delete_message_batch_entry
from platonic.sqs.queue.errors import SQSMessageDoesNotExist
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.sqs import MAX_WAIT_TIME_SECONDS, SQSMixin
from platonic.sqs.queue.types import InternalType, ValueType
from platonic.timeout import InfiniteTimeout
from platonic.timeout.base import BaseTimeout, BaseTimer


@dataclass  # noqa: WPS214
class SQSReceiver(SQSMixin, Receiver[ValueType]):   # noqa: WPS214
    """Queue to read stuff from."""

    timeout: BaseTimeout = field(default_factory=InfiniteTimeout)
    max_wait_time_seconds: int = MAX_WAIT_TIME_SECONDS

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

    def acknowledge(
        self,
        # Liskov Substitution Principle
        message: SQSMessage[ValueType],
    ) -> SQSMessage[ValueType]:
        """
        Acknowledge that the given message was successfully processed.

        Delete a single message from the queue.
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

    def acknowledge_many(
        self,
        messages: Iterable[SQSMessage[ValueType]],
    ) -> None:
        """Remove multiple correctly processed messages from the queue."""
        # FIXME Here, we ignore the success or failure of the request.
        entries = map(generate_delete_message_batch_entry, messages)
        batches = chunked_iter(entries, self.batch_size)
        for batch in batches:
            self.client.delete_message_batch(
                QueueUrl=self.url,
                Entries=batch,
            )

    def __iter__(self) -> Iterator[SQSMessage[ValueType]]:
        """
        Iterate over the messages from the queue.

        If queue is empty, the iterator will, by default, block forever. See
        `SQSReceiver.timeout` argument to change that behavior.
        """
        while True:
            try:
                yield from self._fetch_messages_with_timeout(
                    messages_count=self.batch_size,
                )
            except MessageReceiveTimeout:
                return

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

    def _fetch_messages_with_timeout(
        self,
        messages_count: int,
    ) -> Iterator[SQSMessage[ValueType]]:
        """Within timeout, retrieve the requested number of messages."""
        with self.timeout.timer() as timer:
            while not timer.is_expired:
                try:
                    raw_messages = self._receive_messages(
                        message_count=messages_count,
                        timeout_seconds=self._wait_time_seconds(timer),
                    )['Messages']
                except KeyError:
                    # We have not received any messages. Trying again if we can.
                    continue

                # Messages received, returning them.
                yield from map(
                    self._raw_message_to_sqs_message,
                    raw_messages,
                )
                return

        raise MessageReceiveTimeout(
            queue=self,

            # FIXME MessageReceiveTimeout class should be updated.
            timeout=0,
        )

    def _raw_message_to_sqs_message(
        self, raw_message: MessageTypeDef,
    ) -> SQSMessage[ValueType]:
        """Convert a raw SQS message to the proper SQSMessage instance."""
        # noinspection PyTypeChecker
        return SQSMessage(  # type: ignore
            value=self.deserialize_value(InternalType(
                raw_message['Body'],
            )),
            receipt_handle=raw_message['ReceiptHandle'],
        )

    def _wait_time_seconds(self, timer: BaseTimer) -> int:
        """Based on timer instance, calculate SQS WaitTimeSeconds parameter."""
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
