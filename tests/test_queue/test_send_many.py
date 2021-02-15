from datetime import timedelta

import pytest
from botocore.exceptions import ClientError
from mypy_boto3_sqs import Client as SQSClient

from platonic.queue import MessageReceiveTimeout, QueueDoesNotExist
from tests.test_queue.robot import Command, CommandSender, ReceiverAndSender


class CustomBatchIdSender(CommandSender):
    def _generate_batch_entry_id(self) -> str:
        """Generate a custom, and constant, id."""
        return 'foo'


def test_non_existing_queue(mock_sqs_client: SQSClient):
    """We try to send a command to a queue that does not exist."""
    sender = CommandSender(
        url='https://queue.amazonaws.com/123456789012/non_existing_queue',
    )

    with pytest.raises(QueueDoesNotExist):
        sender.send_many([Command.JUMP])


def test_send_many(receiver_and_sender: ReceiverAndSender):
    """The messages put into queue keep their order."""
    receiver, sender = receiver_and_sender
    receiver.timeout = timedelta(seconds=1)

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    assert receiver.receive().value == Command.RIGHT
    assert receiver.receive().value == Command.FORWARD
    assert receiver.receive().value == Command.LEFT
    assert receiver.receive().value == Command.JUMP

    # No more messages available.
    with pytest.raises(MessageReceiveTimeout):
        receiver.receive()


def test_send_empty_list(receiver_and_sender: ReceiverAndSender):
    """Empty list does not trigger boto3 exception because nothing is sent."""
    receiver, sender = receiver_and_sender

    sender.send_many([])


def test_invalid_batch_entry_id(mock_sqs_client: SQSClient):
    """
    Batch entry id generation can be overwritten.

    We do not cover this with an explicit check. If the client wants
    to override a private method, they should cope with the consequences.
    """
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='robot_commands',
        Attributes={
            'VisibilityTimeout': '3',
        },
    )['QueueUrl']

    sender = CustomBatchIdSender(url=sqs_queue_url)

    with pytest.raises(ClientError):
        sender.send_many([
            Command.JUMP,
            Command.RIGHT,
        ])
