import os

import boto3
import pytest
from moto.sqs import mock_sqs
from mypy_boto3_sqs import Client as SQSClient

from tests.test_queue.robot import (
    CommandReceiver,
    CommandSender,
    ReceiverAndSender,
)


@pytest.fixture(scope='module')
def mock_sqs_client():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    with mock_sqs():
        yield boto3.client('sqs')


@pytest.fixture()
def receiver_and_sender(
    mock_sqs_client: SQSClient,  # noqa: WPS442
) -> ReceiverAndSender:
    """Construct two connected SQS queues."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='robot_commands',
        Attributes={
            'VisibilityTimeout': '60',
        },
    )['QueueUrl']

    sender = CommandSender(url=sqs_queue_url)
    receiver = CommandReceiver(url=sqs_queue_url)

    return receiver, sender


@pytest.fixture(scope='module')
def sqs_queue_url(mock_sqs_client: SQSClient) -> str:
    return mock_sqs_client.create_queue(
        QueueName='test_queue',
        Attributes={
            'VisibilityTimeout': '60',
        },
    )['QueueUrl']
