"""Children are sending letters to Santa Claus. Sometimes those are too long."""
import pytest
from mypy_boto3_sqs import Client as SQSClient
from platonic.queue import MessageTooLarge
from platonic.sqs.queue import SQSReceiver, SQSSender


class LetterSender(SQSSender[str]):
    """Sending letters to Santa Claus."""


class LetterReceiver(SQSReceiver[str]):
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


def test_send_many_individually_large_letters(mock_sqs_client: SQSClient):
    """The message cannot be sent if it exceeds SQS size limits."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='santa_claus_letters',
    )['QueueUrl']

    # Huge letter!
    letter = 'Santa Claus! ' * 100000

    sender = LetterSender(url=sqs_queue_url)

    with pytest.raises(MessageTooLarge):
        sender.send_many([letter] * 5)  # noqa: WPS435


def test_send_many_normal_letters(mock_sqs_client: SQSClient):
    """The message cannot be sent if it exceeds SQS size limits."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='santa_claus_letters',
    )['QueueUrl']

    # Huge letter!
    letter = 'Santa Claus! ' * 100

    sender = LetterSender(url=sqs_queue_url)

    sender.send_many([letter] * 5)  # noqa: WPS435


def test_send_many_letters_as_one_large_batch(mock_sqs_client: SQSClient):
    """The message cannot be sent if it exceeds SQS size limits."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='santa_claus_letters',
    )['QueueUrl']

    # Huge letter!
    letter = 'Santa Claus! ' * 10000

    sender = LetterSender(url=sqs_queue_url)

    sender.send_many([letter] * 10)  # noqa: WPS435
