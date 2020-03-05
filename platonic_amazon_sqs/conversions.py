import json
from typing import TypeVar, Type, NewType, Union
import classes


T = TypeVar('T')
U = TypeVar('U')
JSONString = NewType('JSONString', str)


CONVERTER_BY_DESTINATION_TYPE = {}


def converter(destination_type):
    def registerer(f):
        CONVERTER_BY_DESTINATION_TYPE[destination_type] = f
        return f

    return registerer


@converter(JSONString)
@classes.typeclass
def to_json_string(value: T) -> JSONString:
    ...


@to_json_string.instance(int)
@to_json_string.instance(float)
def numbers_to_json_string(value: Union[int, float]) -> JSONString:
    return JSONString(str(value))


@to_json_string.instance(str)
def str_to_json_string(value: str) -> JSONString:
    return JSONString(json.dumps(value))


@converter(int)
@classes.typeclass
def to_int(value: T) -> int:
    ...


@to_int.instance(str)
def str_to_int(value: str) -> int:
    return int(value)


def convert(value: T, destination_type: Type[U]) -> U:
    """Convert a given value to specified destination type."""
    try:
        converter = CONVERTER_BY_DESTINATION_TYPE[destination_type]

    except KeyError as err:
        raise TypeError(
            f'No converter defined for destination type {destination_type}.'
        ) from err

    try:
        return converter(value)

    except NotImplementedError as err:
        value_type = type(value)
        converter_name = converter._signature

        raise NotImplementedError(
            f'{converter_name} cannot convert {value}: {value_type.__name__} ' +
            f'to {destination_type} because it does not know what to do with ' +
            f'its type.'
        ) from err
