import operator
from itertools import islice

import contexttimer
import pytest
from mypy_boto3_sqs import Client as SQSClient

from platonic.queue import (
    QueueDoesNotExist,
    MessageDoesNotExist,
    MessageReceiveTimeout,
)
from platonic.sqs.queue import SQSMessage
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

    # The visibility timeout for the mock SQS queue is 3 seconds.
    assert round(timer.elapsed) >= 3


def test_send_and_acknowledge(receiver_and_sender: ReceiverAndSender):
    """We receive a command, acknowledge it, and now queue is empty."""
    receiver, sender = receiver_and_sender
    sender.send(Command.JUMP)

    jump_message = receiver.receive()
    receiver.acknowledge(jump_message)

    # Now the queue is empty. We will not be able to receive this message
    # once again.
    with pytest.raises(MessageReceiveTimeout):
        receiver.receive_with_timeout(timeout=2)


def test_acknowledge_fake_message(receiver_and_sender: ReceiverAndSender):
    """Acknowledging a message that does not exist causes an exception."""
    receiver, sender = receiver_and_sender

    message = SQSMessage[Command](
        value=Command.JUMP,
        id='abc',
    )

    with pytest.raises(MessageDoesNotExist):
        receiver.acknowledge(message)


def test_put_many(receiver_and_sender: ReceiverAndSender):
    """The messages put into queue keep their order."""
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    assert receiver.receive().value == Command.RIGHT
    assert receiver.receive().value == Command.FORWARD
    assert receiver.receive().value == Command.LEFT
    assert receiver.receive().value == Command.JUMP

    # No more messages available.
    with pytest.raises(MessageReceiveTimeout):
        receiver.receive_with_timeout(timeout=1)


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


def test_acknowledgement(receiver_and_sender: ReceiverAndSender):
    """
    The acknowledgement() context manager automatically acknowledges messages.
    """
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    received_commands = []
    for message in receiver:
        with receiver.acknowledgement(message):
            received_commands.append(message.value)

        if message.value == Command.JUMP:
            break

    assert received_commands == sent_commands


def test_receive_with_timeout_on_empty_queue(
    receiver_and_sender: ReceiverAndSender,
):
    """Empty queue leads to waiting for a while and empty response."""
    receiver, _sender = receiver_and_sender

    with contexttimer.Timer() as timer:
        with pytest.raises(MessageReceiveTimeout):
            receiver.receive_with_timeout(timeout=1)

    assert round(timer.elapsed) >= 1, 'We have waited for 1 second at least.'
