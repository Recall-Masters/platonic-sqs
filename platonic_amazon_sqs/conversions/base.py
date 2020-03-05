from typing import TypeVar, NewType, Type, Callable, Any, Dict

T = TypeVar('T')
DestinationType = TypeVar('DestinationType')
U = TypeVar('U')

Converter = Callable[[Any], DestinationType]


JSONString = NewType('JSONString', str)
CONVERTER_BY_DESTINATION_TYPE: Dict[Type[DestinationType], Converter] = {}


def converter(destination_type: Type[DestinationType]):
    def registerer(f: Converter):
        CONVERTER_BY_DESTINATION_TYPE[destination_type] = f
        return f

    return registerer


def convert(value: Any, destination_type: Type[DestinationType]) -> U:
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
