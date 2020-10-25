import contexttimer
import pytest

from platonic.queue import MessageReceiveTimeout
from tests.test_queue.robot import Command, ReceiverAndSender


def test_empty_queue(receiver_and_sender: ReceiverAndSender):
    """Empty queue leads to waiting for a while and empty response."""
    receiver, _sender = receiver_and_sender

    with contexttimer.Timer() as timer:
        with pytest.raises(MessageReceiveTimeout):
            receiver.receive_with_timeout(timeout=1)

        elapsed_time = timer.elapsed

    assert round(elapsed_time) >= 1, 'We have waited for 1 second at least.'


def test_non_empty_queue(receiver_and_sender: ReceiverAndSender):
    """Non empty queue leads to immediate response."""
    receiver, sender = receiver_and_sender

    sender.send_many([Command.JUMP, Command.RIGHT])

    with contexttimer.Timer() as timer:
        assert receiver.receive_with_timeout(timeout=5).value == Command.JUMP
        assert receiver.receive_with_timeout(timeout=10).value == Command.RIGHT

        elapsed_time = timer.elapsed

    assert elapsed_time < 1
