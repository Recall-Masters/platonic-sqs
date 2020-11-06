from platonic.sqs.queue import SQSSender


class MyStaticSender(SQSSender[int]):
    """Initialize URL statically."""

    url = 'foo'


class MyDynamicSender(SQSSender[int]):
    """Will initialize URL dynamically."""


def initialize_dynamically():
    """Instantiate a sender class and provide the URL."""
    assert MyDynamicSender(url='...').get_url() == '...'


def test_initialize_statically():
    """Instantiate a sender class where URL is already specified."""
    assert MyStaticSender().get_url() == 'foo'
