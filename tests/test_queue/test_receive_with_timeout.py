from datetime import timedelta

import contexttimer
import pytest

from platonic.queue import MessageReceiveTimeout
from platonic.timeout import ConstantTimeout

from tests.test_queue.robot import Command, ReceiverAndSender


def test_empty_queue(receiver_and_sender: ReceiverAndSender):
    """Empty queue leads to waiting for a while and empty response."""
    receiver, _sender = receiver_and_sender
    receiver.timeout = ConstantTimeout(period=timedelta(seconds=1))

    with contexttimer.Timer() as timer:
        with pytest.raises(MessageReceiveTimeout):
            receiver.receive()

        elapsed_time = timer.elapsed

    assert round(elapsed_time) >= 1, 'We have waited for 1 second at least.'


def test_non_empty_queue(receiver_and_sender: ReceiverAndSender):
    """Non empty queue leads to immediate response."""
    receiver, sender = receiver_and_sender
    receiver.timeout = ConstantTimeout(period=timedelta(seconds=10))

    sender.send_many([Command.JUMP, Command.RIGHT])

    with contexttimer.Timer() as timer:
        assert receiver.receive().value == Command.JUMP
        assert receiver.receive().value == Command.RIGHT

        elapsed_time = timer.elapsed

    assert elapsed_time < 1
