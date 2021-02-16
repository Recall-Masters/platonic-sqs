import pytest
from typecasts import Typecasts, casts

from platonic.sqs.queue import SQSSender
from platonic.sqs.queue.errors import SQSQueueURLNotSpecified

OWN_TYPECASTS = Typecasts()


class MyDynamicSender(SQSSender[int]):
    """Will initialize URL and internal type dynamically."""


def test_initialize_dynamically():
    """Instantiate a sender class and provide the URL."""
    my_casts = Typecasts()
    sender = MyDynamicSender(url='...', internal_type=bytes, typecasts=my_casts)
    assert sender.url == '...'
    assert sender.internal_type == bytes
    assert sender.typecasts == my_casts
