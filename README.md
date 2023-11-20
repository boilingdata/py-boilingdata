# BoilingData Client for Python

```shell
pip install git+https://github.com/boilingdata/py-boilingdata
```

A python client for sending SQL queries and receive results from BoilingData WebSockets API.

- This package uses `asyncio` and `threads` to make the `websocket.WebSocketApp()` run on the background.
- For example usage, please see tests and [main.py](main.py). You can run the main.py with `make run`.
- [`DataQueue class`](py_boilingdata/data_queue.py) is used for book keeping all incoming `DATA` messages and when all pieces are in place, order them and pass back via callback function.

> **NOTE:** This package is considered experimental. Feel free to suggest improvements, especially on how to make this module easy to use.

```python
import os
import asyncio
from py_boilingdata import BoilingData

# os.environ["BD_USERNAME"] = "yourBoilingAccountUsername"
# os.environ["BD_PASSWORD"] = "yourBoilingPassword"

async def main():
    boiling = BoilingData()
    await boiling.connect()
    results = await boiling.execute(
        """
        SELECT first_name, email
          FROM parquet_scan('s3://boilingdata-demo/test.parquet')
         LIMIT 2
        """
    )
    print(results)
    await boiling.close()

asyncio.new_event_loop().run_until_complete(main())
```

## Development

```shell
make install
make build
make lint
make test
make run
pip install dist/py_boilingdata-*-py3-none-any.whl
```
