import operator
from datetime import timedelta
from itertools import islice

from mypy_boto3_sqs import Client as SQSClient
from platonic.timeout import ConstantTimeout

from tests.test_queue.robot import Command, CommandReceiver, ReceiverAndSender


def test_iterate_imperative(receiver_and_sender: ReceiverAndSender):
    """The `for` loop construct can iterate by messages in queue."""
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    received_commands = []
    for message in receiver:
        received_commands.append(message.value)

        receiver.acknowledge(message)

        if message.value == Command.JUMP:
            break

    assert received_commands == sent_commands


def test_iterate_functional(receiver_and_sender: ReceiverAndSender):
    """The `for` loop construct can iterate by messages in queue."""
    receiver, sender = receiver_and_sender

    sent_commands = [Command.RIGHT, Command.FORWARD, Command.LEFT, Command.JUMP]

    sender.send_many(sent_commands)

    # We have to limit the number of messages we are interested in,
    # or .receive() will hang forever.
    messages = islice(receiver, len(sent_commands))

    # Now let's acknowledge those messages.
    messages = map(receiver.acknowledge, messages)

    # And let's see what they are.
    received_commands = list(map(operator.attrgetter('value'), messages))

    assert received_commands == sent_commands


def test_empty_queue(mock_sqs_client: SQSClient):
    """Test empty queue."""
    sqs_queue_url = mock_sqs_client.create_queue(
        QueueName='robot_commands',
        Attributes={
            'VisibilityTimeout': '3',
        },
    )['QueueUrl']

    receiver = CommandReceiver(
        url=sqs_queue_url,
        timeout=ConstantTimeout(period=timedelta(seconds=5))
    )

    for _sqs_message in receiver:
        # Since the queue is empty, this last line will never execute.
        raise ValueError('Queue is not empty!')
