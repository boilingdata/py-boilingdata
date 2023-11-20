import asyncio
from py_boilingdata import BoilingData

boiling = BoilingData()


async def main():
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


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
