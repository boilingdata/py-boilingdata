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
    await asyncio.sleep(10)
    print("DONE.")


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
