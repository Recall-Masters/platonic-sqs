import os

import boto3
from moto.sqs import mock_sqs
from mypy_boto3_sqs import Client as SQSClient
import pytest


@pytest.fixture(scope='function')
def mock_sqs_client():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'

    with mock_sqs():
        yield boto3.client('sqs')
