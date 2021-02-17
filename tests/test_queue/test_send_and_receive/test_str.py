import types
from datetime import timedelta
from typing import Type

import pytest
from platonic.timeout import ConstantTimeout, InfiniteTimeout
from platonic.timeout.base import BaseTimeout

from platonic.sqs.queue import SQSReceiver, SQSSender


@pytest.mark.parametrize('message_value', [
    'foo',
    'boo',
    '<img>x',
])
@pytest.mark.parametrize(
    'sender_class',
    [
        SQSSender[str],
        types.new_class('StrSender', (SQSSender[str], ), {}),
    ],
)
@pytest.mark.parametrize(
    'receiver_class',
    [
        SQSReceiver[str],
        types.new_class('StrReceiver', (SQSReceiver[str], ), {}),
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
def test_send_and_receive_string(
    message_value: str,
    sqs_queue_url: str,

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
