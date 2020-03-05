from typing import TypeVar, NewType, Type, Callable, Any, Dict

JSONString = NewType('JSONString', str)

T = TypeVar('T')

DestinationType = TypeVar('DestinationType')

Converter = Callable[[Any], DestinationType]  # type: ignore


CONVERTER_BY_DESTINATION_TYPE: Dict[Type[T], Converter[T]] = {}  # type: ignore


def converter(destination_type: Type[T]):
    def registerer(f: Converter[T]):
        CONVERTER_BY_DESTINATION_TYPE[destination_type] = f
        return f

    return registerer


def convert(  # type: ignore
    value: Any,
    destination_type: Type[DestinationType]
) -> DestinationType:
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
        converter_name = converter._signature  # type: ignore

        raise NotImplementedError(
            f'{converter_name} cannot convert {value}: {value_type.__name__} ' +
            f'to {destination_type} because it does not know what to do with ' +
            f'its type.'
        ) from err
