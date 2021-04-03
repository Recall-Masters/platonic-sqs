"""This module provides a vocabulary and a few classes to test the SQS queue."""

import operator
from enum import Enum
from typing import Tuple

import typecasts
from platonic.sqs.queue import SQSReceiver, SQSSender


class Command(str, Enum):  # noqa: WPS600
    """Commands for a robot."""

    FORWARD = 'forward'
    RIGHT = 'right'
    LEFT = 'left'
    JUMP = 'jump'


class CommandSender(SQSSender[Command]):
    """Send commands to robot."""


class CommandReceiver(SQSReceiver[Command]):
    """Get commands from the robot."""


typecasts.casts[Command, str] = operator.attrgetter('value')
typecasts.casts[str, Command] = Command


ReceiverAndSender = Tuple[CommandReceiver, CommandSender]
