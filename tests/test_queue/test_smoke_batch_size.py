import pytest
from botocore.exceptions import ClientError
from platonic.sqs.queue import SQSReceiver


def test_smoke_batch_size(sqs_queue_url: str):
    """Smoke test to check maximum allowed batch size for SQS."""
    with pytest.raises(ClientError) as err:
        list(SQSReceiver[str](
            url=sqs_queue_url,
            batch_size=100,
        ))

    assert (
        'An error occurred (InvalidParameterValue) when calling the '
        'ReceiveMessage operation: An error occurred (InvalidParameterValue) '
        'when calling the ReceiveMessage operation: Value 100 for '
        'parameter MaxNumberOfMessages is invalid. Reason: must be between '
        '1 and 10, if provided.'
    ) in str(err.value)
