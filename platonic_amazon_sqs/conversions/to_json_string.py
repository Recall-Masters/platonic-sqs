import json
from typing import NewType, Union

import classes

from platonic_amazon_sqs.conversions.base import converter

JSONString = NewType('JSONString', str)


@converter(JSONString)
@classes.typeclass
def to_json_string(value) -> JSONString:
    """Convert anything to JSON string."""


@to_json_string.instance(int)
@to_json_string.instance(float)
def numbers_to_json_string(value: Union[int, float]) -> JSONString:
    """Numeric values convert to JSON as is."""
    return JSONString(str(value))


@to_json_string.instance(str)
def str_to_json_string(value: str) -> JSONString:
    """String value must be properly escaped."""
    return JSONString(json.dumps(value))
