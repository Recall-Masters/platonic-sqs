import json
import uuid
from functools import reduce
from typing import Iterable, List

from botocore.exceptions import ClientError
from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef
from platonic.queue import MessageTooLarge, Sender
from platonic.sqs.queue.errors import SQSQueueDoesNotExist
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.sqs import MAX_MESSAGE_SIZE, SQSMixin
from platonic.sqs.queue.types import ValueType


def _error_code_is(error: ClientError, error_code: str) -> bool:
    """Check error code of a boto3 ClientError."""
    return error.response['Error']['Code'] == error_code


class SQSSender(SQSMixin, Sender[ValueType]):
    """Queue to write stuff into."""

    def send(self, instance: ValueType) -> SQSMessage[ValueType]:
        """Put a message into the queue."""
        message_body = self.serialize_value(instance)

        try:
            sqs_response = self.client.send_message(
                QueueUrl=self.url,
                MessageBody=message_body,
            )

        except self.client.exceptions.QueueDoesNotExist as queue_does_not_exist:
            raise SQSQueueDoesNotExist(queue=self) from queue_does_not_exist

        except self.client.exceptions.ClientError as err:
            if _error_code_is(err, 'InvalidParameterValue'):
                raise MessageTooLarge(
                    max_supported_size=MAX_MESSAGE_SIZE,
                    message_body=message_body,
                )

            raise  # pragma: no cover

        return SQSMessage(  # type: ignore
            value=instance,
            # FIXME this probably is not correct. `id` contains MessageId in
            #   one cases and ResponseHandle in others. Inconsistent.
            receipt_handle=sqs_response['MessageId'],
        )

    def send_many(self, iterable: Iterable[ValueType]) -> None:
        """Send multiple messages."""
        send_batch_entries = map(
            self._generate_send_batch_entry,
            iterable,
        )

        trailing_entries: List[SendMessageBatchRequestEntryTypeDef] = reduce(
            self._accumulate_batch_for_sending,
            send_batch_entries,
            [],
        )

        # The last batch returned from reduce() is sendable (see postcondition
        # for `_accumulate_batch_for_sending()` function), and it was not sent
        # by _accumulate_batch_for_sending() itself. We are at the end of the
        # entries sequence, we have to send it out.
        if trailing_entries:
            self._send_message_batch(trailing_entries)

    def _send_message_batch(
        self,
        entries: List[SendMessageBatchRequestEntryTypeDef],
    ):
        try:
            self.client.send_message_batch(
                QueueUrl=self.url,
                Entries=entries,
            )

        except self.client.exceptions.QueueDoesNotExist as does_not_exist:
            raise SQSQueueDoesNotExist(queue=self) from does_not_exist

        except self.client.exceptions.ClientError as err:
            if _error_code_is(err, 'BatchRequestTooLong'):   # pragma: no cover
                raise MessageTooLarge(
                    max_supported_size=MAX_MESSAGE_SIZE,
                    message_body=json.dumps(entries),
                )

            raise

    def _accumulate_batch_for_sending(
        self,
        existing_entries: List[SendMessageBatchRequestEntryTypeDef],
        new_entry: SendMessageBatchRequestEntryTypeDef,
    ) -> List[SendMessageBatchRequestEntryTypeDef]:
        """
        Analyse a batch of entries plus a new one.

        If the new entry can be appended to the batch, and it still will be
        eligible for sending out to the queue, do that.

        Otherwise, send the batch while we can, and start a new one which will
        only contain the new_entry in it.

        Precondition: existing_entries is a batch that is eligible to be
        sent out, because we verified that on a previous iteration.

        Postcondition: the list returned by this function is
            - not empty,
            - eligible to be sent out.
        """
        new_batch_size = sum(
            len(entry['MessageBody'])
            for entry in existing_entries + [new_entry]
        )

        if new_batch_size > MAX_MESSAGE_SIZE:
            # We cannot add new entry to the existing batch because it will be
            # too large. Let's send it out.
            if existing_entries:
                self._send_message_batch(existing_entries)
            else:
                raise MessageTooLarge(
                    max_supported_size=MAX_MESSAGE_SIZE,
                    message_body=new_entry['MessageBody'],
                )

            return [new_entry]

        new_batch_count = len(existing_entries) + 1
        if new_batch_count > self.batch_size:
            # We cannot add a new entry to this batch because its entry count
            # is already at the max limit. Sending it out.
            self._send_message_batch(existing_entries)
            return [new_entry]

        # No problems found. Adding new entry to the batch! Perhaps we will send
        # it on next iteration.
        return existing_entries + [new_entry]

    def _generate_batch_entry_id(self) -> str:
        """Generate batch entry id."""
        return uuid.uuid4().hex

    def _generate_send_batch_entry(
        self,
        instance: ValueType,
    ) -> SendMessageBatchRequestEntryTypeDef:
        """Compose the entry for send_message_batch() operation."""
        return SendMessageBatchRequestEntryTypeDef(
            Id=self._generate_batch_entry_id(),
            MessageBody=self.serialize_value(instance),
        )
