import asyncio
from py_boilingdata import BoilingData
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env
# AWS_REGION  = "eu-west-1"
# BD_USERNAME = "yourBoilingAccountUsername"
# BD_PASSWORD = "yourBoilingPassword"


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
