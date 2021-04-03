import uuid

from mypy_boto3_sqs.type_defs import DeleteMessageBatchRequestEntryTypeDef
from platonic.sqs.queue.message import SQSMessage
from platonic.sqs.queue.types import ValueType


def _generate_delete_message_batch_entry_id() -> str:
    """Generate batch entry Id."""
    return uuid.uuid4().hex


def generate_delete_message_batch_entry(
    message: SQSMessage[ValueType],
) -> DeleteMessageBatchRequestEntryTypeDef:
    """Convert a Message into an entry for DeleteMessageBatch operation."""
    return {
        'Id': _generate_delete_message_batch_entry_id(),
        'ReceiptHandle': message.receipt_handle,
    }
