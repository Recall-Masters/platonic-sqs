import itertools
from typing import Tuple

import contexttimer
import pytest
from moto.sqs import mock_sqs
from mypy_boto3_sqs import Client as SQSClient

from platonic import QueueDoesNotExist, MessageDoesNotExist
from platonic_amazon_sqs import SQSMessage
from tests.test_queue.robot import (
    Command, CommandSender, CommandReceiver,
    ReceiverAndSender,
)


@pytest.fixture
def receiver_and_sender(
    mock_sqs_client: SQSClient,
) -> ReceiverAndSender:
    """Construct two connected SQS queues."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='robot_commands',
        Attributes={
            'VisibilityTimeout': '3',
        }
    )['QueueUrl']

    sender = CommandSender(url=sqs_queue_url)
    receiver = CommandReceiver(url=sqs_queue_url)

    return receiver, sender


def test_non_existing_queue(mock_sqs_client: SQSClient):
    """We try to send a command to a queue that does not exist."""
    sender = CommandSender(
        url='https://queue.amazonaws.com/123456789012/non_existing_queue',
    )

    with pytest.raises(QueueDoesNotExist):
        sender.put(Command.JUMP)


def test_put_and_get(receiver_and_sender: ReceiverAndSender):
    """Whatever we put into sender ends up in receiver."""
    receiver, sender = receiver_and_sender
    sender.put(Command.RIGHT)
    assert receiver.get().value == Command.RIGHT


def test_put_and_get_twice(receiver_and_sender: ReceiverAndSender):
    """We have not acknowledged the message. Thus we will receive it again."""
    receiver, sender = receiver_and_sender
    sender.put(Command.RIGHT)

    # We are receiving the command successfully.
    assert receiver.get().value == Command.RIGHT

    # This call will return the message again as soon as visibility timeout
    # expires.
    with contexttimer.Timer() as timer:
        assert receiver.get().value == Command.RIGHT

    # The visibility timeout for the mock SQS queue is 3 seconds.
    assert round(timer.elapsed) >= 3


def test_put_and_acknowledge(receiver_and_sender: ReceiverAndSender):
    """We receive a command, acknowledge it, and now queue is empty."""
    receiver, sender = receiver_and_sender
    sender.put(Command.JUMP)

    jump_message = receiver.get()
    receiver.acknowledge(jump_message)
    # If we run another receiver.get() - it will hang indefinitely.


def test_acknowledge_fake_message(receiver_and_sender: ReceiverAndSender):
    """Acknowledging a message that does not exist causes an exception."""
    receiver, sender = receiver_and_sender

    message = SQSMessage(
        value=Command.JUMP,
        id='abc',
    )

    with pytest.raises(MessageDoesNotExist):
        receiver.acknowledge(message)
