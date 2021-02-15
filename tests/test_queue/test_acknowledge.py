from datetime import timedelta

import pytest

from platonic.queue import MessageDoesNotExist, MessageReceiveTimeout
from platonic.sqs.queue import SQSMessage
from tests.test_queue.robot import Command, ReceiverAndSender


def test_send_and_acknowledge(receiver_and_sender: ReceiverAndSender):
    """We receive a command, acknowledge it, and now queue is empty."""
    receiver, sender = receiver_and_sender
    receiver.timeout = timedelta(seconds=2)

    sender.send(Command.JUMP)

    jump_message = receiver.receive()
    receiver.acknowledge(jump_message)

    # Now the queue is empty. We will not be able to receive this message
    # once again.
    with pytest.raises(MessageReceiveTimeout):
        receiver.receive()


def test_acknowledge_fake_message(receiver_and_sender: ReceiverAndSender):
    """Acknowledging a message that does not exist causes an exception."""
    receiver, sender = receiver_and_sender

    message = SQSMessage[Command](
        value=Command.JUMP,
        receipt_handle='abc',
    )

    with pytest.raises(MessageDoesNotExist):
        receiver.acknowledge(message)


def test_acknowledgement(receiver_and_sender: ReceiverAndSender):
    """The context manager automatically acknowledges messages."""
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
