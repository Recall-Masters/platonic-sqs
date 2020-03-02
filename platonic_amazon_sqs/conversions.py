import json
from typing import TypeVar, Type

T = TypeVar('T')


def to_string(value: T) -> str:
    ...


def from_string(destination_type: Type[T], raw_value: str) -> T:
    if destination_type is int:
        return int(raw_value)

    elif destination_type is str:
        return raw_value

    elif destination_type is dict:
        return json.loads(raw_value)

    else:
        raise ValueError(f'How to coerce {raw_value} to {destination_type}?')
