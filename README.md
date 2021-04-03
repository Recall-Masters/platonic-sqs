# platonic-sqs

[![Coverage](https://coveralls.io/repos/github/python-platonic/platonic-sqs/badge.svg?branch=master)](https://coveralls.io/github/python-platonic/platonic-sqs?branch=master)
[![Python Version](https://img.shields.io/pypi/pyversions/platonic-sqs.svg)](https://pypi.org/project/platonic-sqs/)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)

A [platonic](https://platonic.tools) wrapper for Amazon SQS queue service. Abstracts away the implementation details of SQS, providing you with a clean and typed queue interface.


## Installation

```bash
pip install platonic-sqs
```


## Send

```python
from platonic.sqs.queue import SQSSender

numbers_out = SQSSender[int](
    url='https://sqs.us-west-2.amazonaws.com/123456789012/queue-name',
)

numbers_out.send(15)
numbers_out.send_many([1, 1, 2, 3, 5, 8, 13])
```

## Receive & acknowledge

```python
from platonic.sqs.queue import SQSReceiver
from platonic.timeout import ConstantTimeout
from datetime import timedelta

incoming_numbers = SQSReceiver[int](
    url='https://sqs.us-west-2.amazonaws.com/123456789012/queue-name',
    # Thus we prevent the receiver from blocking forever if queue is empty
    timeout=ConstantTimeout(period=timedelta(minutes=3)),
)

# If the queue is empty, this call with block until there is a message.
cmd = incoming_numbers.receive()

assert cmd.value == 15
# Do complicated stuff with the value
print(cmd.value * 1234 + 5767)

incoming_numbers.acknowledge(cmd)
```

## License

[MIT](https://github.com/python-platonic/platonic-sqs/blob/master/LICENSE)


## Credits

* This project was generated with [`wemake-python-package`](https://github.com/wemake-services/wemake-python-package). Current template version is: [c9e9ea8b9be2464cacd00b9c2a438e821da9121b](https://github.com/wemake-services/wemake-python-package/tree/c9e9ea8b9be2464cacd00b9c2a438e821da9121b). See what is [updated](https://github.com/wemake-services/wemake-python-package/compare/c9e9ea8b9be2464cacd00b9c2a438e821da9121b...master) since then.
* This project is partially sponsored by [Recall Masters](https://github.com/Recall-Masters) - look out for issues with `sponsored` tag.
