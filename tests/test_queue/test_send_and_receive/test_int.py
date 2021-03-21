import types
from datetime import timedelta
from typing import Type

import pytest
import typecasts

from platonic.sqs.queue import SQSReceiver, SQSSender
from platonic.timeout import ConstantTimeout, InfiniteTimeout
from platonic.timeout.base import BaseTimeout

# FIXME Remove int vs str casts when implemented in typecasts library
typecasts.casts[int, str] = str
typecasts.casts[str, int] = int


class IntSender(SQSSender[int]):
    """Integer sender."""


class IntReceiver(SQSReceiver[int]):
    """Integer receiver."""


@pytest.mark.parametrize('message_value', [0, 1, 365465477])
@pytest.mark.parametrize(
    'sender_class',
    [
        SQSSender[int],
        IntSender,
    ],
)
@pytest.mark.parametrize(
    'receiver_class',
    [
        SQSReceiver[int],
        IntReceiver,
    ],
)
@pytest.mark.parametrize(
    'timeout',
    [
        InfiniteTimeout(),
        ConstantTimeout(period=timedelta(seconds=5)),
        ConstantTimeout(period=timedelta(minutes=5)),
    ],
)
def test_send_and_receive_int(
    message_value: int,
    sqs_queue_url: int,

    sender_class: Type[SQSSender],
    receiver_class: Type[SQSReceiver],

    timeout: BaseTimeout,
):
    """Send a message and get it back."""
    sender: SQSSender = sender_class(url=sqs_queue_url)
    receiver: SQSReceiver = receiver_class(
        url=sqs_queue_url,
        timeout=timeout,
    )

    sender.send(message_value)

    assert receiver.receive().value == message_value
