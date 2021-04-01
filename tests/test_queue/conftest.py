import os
from datetime import timedelta

import boto3
import pytest
from moto.sqs import mock_sqs
from mypy_boto3_sqs import Client as SQSClient
from platonic.sqs.queue import SQSReceiver, SQSSender
from platonic.timeout import ConstantTimeout
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
    os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

    with mock_sqs():
        yield boto3.client('sqs')


@pytest.fixture()
def receiver_and_sender(
    sqs_queue_url: str,
) -> ReceiverAndSender:
    """Construct two connected SQS queues."""
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


@pytest.fixture()
def str_receiver_with_constant_timeout(sqs_queue_url: str) -> SQSReceiver:
    """Receiver with timeout of 25 seconds."""
    return SQSReceiver[str](
        url=sqs_queue_url,
        timeout=ConstantTimeout(period=timedelta(seconds=25)),
    )


@pytest.fixture()
def str_sender(sqs_queue_url: str) -> SQSSender:
    """Sender."""
    return SQSSender[str](
        url=sqs_queue_url,
    )
