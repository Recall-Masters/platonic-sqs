from datetime import timedelta
from typing import Type

import pytest
from platonic.sqs.queue import SQSReceiver, SQSSender
from platonic.timeout import ConstantTimeout, InfiniteTimeout
from platonic.timeout.base import BaseTimeout


class StrSender(SQSSender[str]):
    """String sender."""


class StrReceiver(SQSReceiver[str]):
    """String receiver."""


@pytest.mark.parametrize('message_value', [
    'foo',
    'boo',
    '<img>x',
])
@pytest.mark.parametrize(
    'sender_class',
    [
        SQSSender[str],
        StrSender,
    ],
)
@pytest.mark.parametrize(
    'receiver_class',
    [
        SQSReceiver[str],
        StrReceiver,
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
