import classes

from platonic_amazon_sqs.conversions.base import converter


@converter(int)
@classes.typeclass
def to_int(value) -> int:
    """Convert things to integer."""


@to_int.instance(str)
def str_to_int(value: str) -> int:
    """Convert string to int and pray it works."""
    return int(value)
