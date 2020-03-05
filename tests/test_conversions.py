import pytest

from platonic_amazon_sqs import conversions

DATA = (
    (5, conversions.JSONString, '5'),
    (5.3, conversions.JSONString, '5.3'),
    ('abc', conversions.JSONString, '"abc"'),
    ('5', int, 5),
)


@pytest.mark.parametrize(('value', 'destination_type', 'result'), DATA)
def test_convert(value, destination_type, result):
    assert conversions.convert(value, destination_type) == result
