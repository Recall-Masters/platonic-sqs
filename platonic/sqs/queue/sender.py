import json
import uuid
from typing import Iterable

from boltons.iterutils import chunked_iter
from botocore.exceptions import ClientError
from mypy_boto3_sqs.type_defs import SendMessageBatchRequestEntryTypeDef

from platonic.queue import MessageTooLarge, Sender
from platonic.sqs.queue.errors import SQSQueueDoesNotExist
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.sqs import (
    MAX_MESSAGE_SIZE,
    MAX_NUMBER_OF_MESSAGES,
    SQSMixin,
)
from platonic.sqs.queue.types import ValueType


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
            if self._error_code_is(err, 'InvalidParameterValue'):
                raise MessageTooLarge(
                    max_supported_size=MAX_MESSAGE_SIZE,
                    message_body=message_body,
                )

            raise  # pragma: no cover

        return SQSMessage(
            value=instance,
            # FIXME this probably is not correct. `id` contains MessageId in
            #   one cases and ResponseHandle in others. Inconsistent.
            receipt_handle=sqs_response['MessageId'],
        )

    def send_many(self, iterable: Iterable[ValueType]) -> None:  # noqa: WPS231
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

            except self.client.exceptions.QueueDoesNotExist as does_not_exist:
                raise SQSQueueDoesNotExist(queue=self) from does_not_exist

            except self.client.exceptions.ClientError as err:
                if self._error_code_is(err, 'BatchRequestTooLong'):
                    raise MessageTooLarge(
                        max_supported_size=MAX_MESSAGE_SIZE,
                        message_body=json.dumps(entries),
                    )

                raise

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

    def _error_code_is(self, error: ClientError, error_code: str) -> bool:
        """Check error code of a boto3 ClientError."""
        return error.response['Error']['Code'] == error_code
