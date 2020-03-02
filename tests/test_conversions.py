from platonic_amazon_sqs import conversions


def test_str():
    assert conversions.from_string(str, '5') == '5'


def test_int():
    assert conversions.from_string(int, '5') == 5
