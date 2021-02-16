import pytest
from typecasts import Typecasts, casts

from platonic.sqs.queue import SQSSender
from platonic.sqs.queue.errors import SQSQueueURLNotSpecified

OWN_TYPECASTS = Typecasts()


class MyStaticSender(SQSSender[int]):
    """Initialize URL and internal type statically."""

    url = 'foo'
    internal_type = bytes


class MyStaticTypecastsSender(SQSSender[int]):
    """Initialize URL and internal type statically."""

    url = 'foo'
    typecasts = OWN_TYPECASTS


class MyDynamicSender(SQSSender[int]):
    """Will initialize URL and internal type dynamically."""


def test_no_url():
    with pytest.raises(TypeError) as err:
        MyDynamicSender()

    assert 'sqs' in str(err.value)  # noqa: WPS441


def test_initialize_typecasts():
    """Typecasts."""
    sender = MyStaticTypecastsSender(url='boo')
    assert sender.typecasts == OWN_TYPECASTS


def test_initialize_dynamically():
    """Instantiate a sender class and provide the URL."""
    my_casts = Typecasts()
    sender = MyDynamicSender(url='...', internal_type=bytes, typecasts=my_casts)
    assert sender.url == '...'
    assert sender.internal_type == bytes
    assert sender.typecasts == my_casts


def test_initialize_statically():
    """Instantiate a sender class where URL is already specified."""
    sender = MyStaticSender()
    assert sender.url == 'foo'
    assert sender.internal_type == bytes
    assert sender.typecasts == casts
    assert sender.client is not None
