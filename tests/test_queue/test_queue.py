import dataclasses
import json
from mypy_boto3_sqs import Client as SQSClient

import typecasts
from moto.sqs import mock_sqs

from platonic_amazon_sqs import SQSOutputQueue, SQSInputQueue


@dataclasses.dataclass
class Wizard:
    name: str
    rank: int


typecasts.casts[Wizard, str] = lambda wizard: json.dumps(
    dataclasses.asdict(wizard),
)


typecasts.casts[str, Wizard] = lambda raw_wizard: Wizard(**json.loads(
    raw_wizard,
))


@dataclasses.dataclass
class OutWizards(SQSOutputQueue[Wizard]):
    """Send wizards into a queue."""


@dataclasses.dataclass
class InWizards(SQSInputQueue[Wizard]):
    """Get wizards from a queue."""


@mock_sqs
def test_queue(mock_sqs_client: SQSClient):
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='wizards',
    )['QueueUrl']

    out = OutWizards(url=sqs_queue_url)

    rincewind_message = out.put(Wizard(
        name='Rincewind',
        rank=0,
    ))
    assert rincewind_message.id != ''

    inp = InWizards(url=sqs_queue_url)
    received_message = inp.get()
    assert received_message == rincewind_message
