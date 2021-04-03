import contexttimer
import pytest
from mypy_boto3_sqs import Client as SQSClient
from platonic.queue import QueueDoesNotExist
from tests.test_queue.robot import Command, CommandSender, ReceiverAndSender


def test_non_existing_queue(mock_sqs_client: SQSClient):
    """We try to send a command to a queue that does not exist."""
    sender = CommandSender(
        url='https://queue.amazonaws.com/123456789012/non_existing_queue',
    )

    with pytest.raises(QueueDoesNotExist):
        sender.send(Command.JUMP)


def test_send_and_receive(receiver_and_sender: ReceiverAndSender):
    """Whatever we put into sender ends up in receiver."""
    receiver, sender = receiver_and_sender
    sender.send(Command.RIGHT)
    assert receiver.receive().value == Command.RIGHT


def test_send_once_and_receive_twice(receiver_and_sender: ReceiverAndSender):
    """We have not acknowledged the message. Thus we will receive it again."""
    receiver, sender = receiver_and_sender
    sender.send(Command.RIGHT)

    # We are receiving the command successfully.
    assert receiver.receive().value == Command.RIGHT

    # This call will return the message again as soon as visibility timeout
    # expires.
    with contexttimer.Timer() as timer:
        assert receiver.receive().value == Command.RIGHT

        elapsed_time = timer.elapsed

    # The visibility timeout for the mock SQS queue is 3 seconds.
    assert round(elapsed_time) >= 3
