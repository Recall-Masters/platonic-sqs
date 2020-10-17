"""Children are sending letters to Santa Claus. Sometimes those are too long."""
import pytest

from platonic.queue import MessageTooLarge
from platonic_amazon_sqs.queue import SQSOutputQueue, SQSInputQueue
from mypy_boto3_sqs import Client as SQSClient


class LetterSender(SQSOutputQueue[str]):
    """Sending letters to Santa Claus."""


class LetterReceiver(SQSInputQueue[str]):
    """Receiving letters from children."""


def test_send_large_letter(mock_sqs_client: SQSClient):
    """The message cannot be sent if it exceeds SQS size limits."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='santa_claus_letters',
    )['QueueUrl']

    # Huge letter!
    letter = 'Santa Claus! ' * 100000

    sender = LetterSender(url=sqs_queue_url)

    with pytest.raises(MessageTooLarge):
        sender.send(letter)


def test_send_many_large_letters(mock_sqs_client: SQSClient):
    """The message cannot be sent if it exceeds SQS size limits."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='santa_claus_letters',
    )['QueueUrl']

    # Huge letter!
    letter = 'Santa Claus! ' * 100000

    sender = LetterSender(url=sqs_queue_url)

    with pytest.raises(MessageTooLarge):
        sender.send_many([letter] * 5)
