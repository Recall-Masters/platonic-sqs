import dataclasses
import typing
from abc import ABC
from typing import Generic, Optional, Type, TypeVar

import boto3
from boto3_type_annotations import sqs

from platonic_amazon_sqs import conversions
from platonic_amazon_sqs.conversions import JSONString, convert

ValueType = TypeVar('ValueType')


@dataclasses.dataclass(frozen=True)
class Message(Generic[ValueType]):
    """Container of a value pulled from queue."""

    value: ValueType  # noqa: WPS110


def get_type_args(instance: ValueType) -> typing.Tuple[type, ...]:
    """Get type arguments of an object's class."""
    for parent in instance.__orig_bases__:  # type: ignore  # noqa: WPS609
        type_args = typing.get_args(parent)
        if type_args:
            return type_args

    return ()


class AcknowledgementQueue(Generic[ValueType], ABC):
    """This will go to `platonic`."""

    _value_type: Type[ValueType]

    @property
    def value_type(self) -> Type[ValueType]:
        """Queue element type."""
        try:
            return self._value_type

        except AttributeError:
            args = get_type_args(self)
            (self._value_type, ) = args  # noqa: WPS414
            return self._value_type

    def put(self, value: ValueType):  # noqa: WPS110
        """Add an item to the queue."""

    def get(self) -> Message[ValueType]:
        """Get item from queue."""

    def acknowledge(self, message: Message[ValueType]):
        """Acknowledge that the message was processed."""


@dataclasses.dataclass(frozen=True)
class SQSMessage(Message[ValueType]):
    """SQS message houses unique message ID."""

    id: str


class SQSQueue(AcknowledgementQueue[ValueType]):
    """SQS backend for a queue."""

    url: str
    max_number_of_messages: int

    def __init__(
        self,
        url: Optional[str] = None,
        max_number_of_messages: Optional[int] = None,
    ):
        if url is not None:
            self.url = url

        if max_number_of_messages is not None:
            self.max_number_of_messages = max_number_of_messages
        else:
            self.max_number_of_messages = 10

    @property
    def client(self) -> sqs.Client:
        """Get AWS SQS client."""
        # FIXME make this a cached property
        return boto3.client('sqs')

    def serialize(self, value: ValueType) -> JSONString:  # noqa: WPS110
        """Convert queue value type to JSON string."""
        return convert(
            value=value,
            destination_type=JSONString,
        )

    def deserialize(self, raw_value: JSONString) -> ValueType:
        """Convert JSON string value to the queue value type."""
        return conversions.convert(
            value=raw_value,
            destination_type=self.value_type,
        )

    def put(self, value: ValueType):  # noqa: WPS110
        """Send message to the queue."""
        self.client.send_message(
            QueueUrl=self.url,
            MessageBody=self.serialize(value),
        )
