"""This module provides a vocabulary and a few classes to test the SQS queue."""

import operator
from enum import Enum
from typing import Tuple

import typecasts

from platonic.sqs.queue import SQSOutputQueue, SQSInputQueue


class Command(str, Enum):
    """Commands for a robot."""

    FORWARD = 'forward'
    RIGHT = 'right'
    LEFT = 'left'
    JUMP = 'jump'


class CommandSender(SQSOutputQueue[Command]):
    """Send commands to robot."""


class CommandReceiver(SQSInputQueue[Command]):
    """Get commands from the robot."""


typecasts.casts[Command, str] = operator.attrgetter('value')
typecasts.casts[str, Command] = Command


ReceiverAndSender = Tuple[CommandReceiver, CommandSender]
