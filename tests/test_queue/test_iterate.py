import operator
from itertools import islice

import contexttimer
import pytest
from mypy_boto3_sqs import Client as SQSClient

from tests.test_queue.robot import Command, CommandReceiver, ReceiverAndSender


def test_pause(receiver_and_sender: ReceiverAndSender):
    """Wait a bit if the queue is empty."""
    receiver, _sender = receiver_and_sender

    assert receiver.iteration_timeout == 3

    with contexttimer.Timer() as timer:
        receiver._pause_while_iterating_over_queue()

        elapsed_time = timer.elapsed

    assert round(elapsed_time) <= receiver.iteration_timeout


def test_iterate_imperative(receiver_and_sender: ReceiverAndSender):
    """The `for` loop construct can iterate by messages in queue."""
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    received_commands = []
    for message in receiver:
        received_commands.append(message.value)

        receiver.acknowledge(message)

        if message.value == Command.JUMP:
            break

    assert received_commands == sent_commands


def test_iterate_functional(receiver_and_sender: ReceiverAndSender):
    """The `for` loop construct can iterate by messages in queue."""
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    # We have to limit the number of messages we are interested in,
    # or .receive() will hang forever.
    messages = islice(receiver, len(sent_commands))

    # Now let's acknowledge those messages.
    messages = map(receiver.acknowledge, messages)

    # And let's see what they are.
    received_commands = list(map(operator.attrgetter('value'), messages))

    assert received_commands == sent_commands


class NotPausingReceiver(CommandReceiver):
    """Receiver with overridden pause method."""

    def _pause_while_iterating_over_queue(self) -> None:
        """Do not pause, raise instead."""
        raise ValueError('foo!')


def test_empty_queue(mock_sqs_client: SQSClient):
    """Test empty queue."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='robot_commands',
        Attributes={
            'VisibilityTimeout': '3',
        },
    )['QueueUrl']

    receiver = NotPausingReceiver(url=sqs_queue_url)

    with pytest.raises(ValueError):
        for sqs_message in receiver:
            assert sqs_message is not None
