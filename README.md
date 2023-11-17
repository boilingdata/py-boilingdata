# BoilingData Client for Python

A python client for sending SQL queries and receive results from BoilingData WebSockets API.

- This package uses `asyncio` and `threads` to make the `websocket.WebSocketApp()` run on the background.
- For example usage, please see tests and [main.py](main.py). You can run the main.py with `make run`.
- [`DataQueue class`](py_boilingdata/data_queue.py) is used for book keeping all incoming `DATA` messages and when all pieces are in place, order them and pass back via callback function.

> **NOTE:** This package is considered experimental. Feel free to suggest improvements, especially on how to make this module easy to use.

```python
import asyncio
from pprint import pprint
from py_boilingdata import BoilingData


async def main():
    boiling = BoilingData()
    await boiling.connect()
    await boiling.populate()

    def cb(resp):
        pprint(resp)

    q = "SELECT first_name, email FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 10"
    await boiling.execute(q, cb)
    await asyncio.sleep(10) # There is no way to await for the cb call..
    print("DONE.")

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## Build and Install

```shell
make install
make build
# make lint
# make test
pip install dist/py_boilingdata-*-py3-none-any.whl
```
