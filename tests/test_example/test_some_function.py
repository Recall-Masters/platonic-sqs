# -*- coding: utf-8 -*-

from platonic_amazon_sqs.queue import SQSQueue


class Ages(SQSQueue[int]):
    url = ''


def test_value_type():
    ages = Ages()
    assert ages.value_type is int
