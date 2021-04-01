import string

import contexttimer
from platonic.sqs.queue import SQSReceiver, SQSSender


def test_send_and_acknowledge_many(  # noqa: WPS210
    str_receiver_with_constant_timeout: SQSReceiver,
    str_sender: SQSSender,
):
    # Send every single letter to the queue.
    letter_values = list(string.ascii_uppercase)
    str_sender.send_many(letter_values)

    with contexttimer.Timer() as timer:  # noqa: WPS440
        messages = list(str_receiver_with_constant_timeout)
        elapsed_time = timer.elapsed

    assert elapsed_time > 24
    assert elapsed_time < 26

    assert len(messages) == len(letter_values)

    str_receiver_with_constant_timeout.acknowledge_many(messages)

    with contexttimer.Timer() as timer:   # noqa: WPS440
        empty_messages = list(str_receiver_with_constant_timeout)
        empty_elapsed_time = timer.elapsed

    assert not empty_messages
    assert empty_elapsed_time > 24
    assert empty_elapsed_time < 26
