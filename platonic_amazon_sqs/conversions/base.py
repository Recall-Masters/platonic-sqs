from typing import Any, Callable, Dict, Type, TypeVar

T = TypeVar('T')  # noqa: WPS111

DestinationType = TypeVar('DestinationType')

Converter = Callable[[Any], DestinationType]  # type: ignore


CONVERTER_BY_DESTINATION_TYPE: Dict[  # type: ignore  # noqa: WPS407
    Type[T], Converter[T],
] = {}


def converter(destination_type: Type[T]):
    """Register function as a converter to certain destination type."""
    def registerer(function: Converter[T]):  # noqa: WPS430
        CONVERTER_BY_DESTINATION_TYPE[destination_type] = function
        return function

    return registerer


def convert(  # type: ignore
    value: Any,
    destination_type: Type[DestinationType],
) -> DestinationType:
    """Convert a given value to specified destination type."""
    try:
        concrete_converter = CONVERTER_BY_DESTINATION_TYPE[destination_type]

    except KeyError as key_error:
        raise TypeError(
            f'No converter defined for destination type {destination_type}.',
        ) from key_error

    try:
        return concrete_converter(value)

    except NotImplementedError as not_implemented_error:
        value_type = type(value)
        converter_name = converter._signature  # type: ignore  # noqa: WPS437

        raise NotImplementedError(
            f'{converter_name} cannot convert {value}: {value_type.__name__} ' +
            f'to {destination_type} because it does not know what to do with ' +
            f'its type.',
        ) from not_implemented_error
