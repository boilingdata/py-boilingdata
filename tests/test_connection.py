"""BoilinData WebSocket client tests"""
import pytest
import pytest_asyncio
import asyncio
from py_boilingdata import BoilingData

boiling = BoilingData()

##
## Unmocked tests against BoilingData
##


# We setup the session once and close it after all tests
@pytest_asyncio.fixture(scope="session", autouse=True)
async def manage_boiling_connection():
    await boiling.connect()
    await boiling.populate()
    yield None
    await boiling.close()


# @pytest.mark.asyncio
# async def test_local_query():
#     """Simple SQL statement runs locally with embedded DuckDB"""

#     def cb(data):
#         assert data == [(1,)]

#     await boiling.execute("SELECT 1;", cb)


# @pytest.mark.asyncio
# async def test_local_tables():
#     """We should have Boiling Data Catalog entries"""

#     # Note: This depends on the used BD_USERNAME but also data sets shared
#     def cb(resp):
#         assert resp == [
#             ("demo_full",),
#             ("taxi_locations",),
#             ("taxi_locations_limited",),
#         ]

#     await boiling.execute("SHOW TABLES;", cb)


@pytest.mark.asyncio
async def test_bd_query():
    """Small response from Boiling"""

    # Note: This depends on the used BD_USERNAME but also data sets shared
    def cb(resp):
        assert resp == [{"email": "ajordan0@com.com", "first_name": "Amanda"}]

    q = "SELECT first_name, email FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 1"
    await boiling.execute(q, cb)
    assert True
    # counter = 0
    # while not done and counter < 10:
    #     await asyncio.sleep(1)
    #     counter = counter + 1
    #     print(".")


# @pytest.mark.asyncio
# async def test_bd_query_large(snapshot):
#     """Bigger response from Boiling"""

#     # Note: This depends on the used BD_USERNAME but also data sets shared
#     def cb(resp):
#         snapshot.assert_match(str(resp), "test_bd_query_large")

#     q = "SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, PULocationID, DOLocationID LIMIT 200"
#     await boiling.execute(q, cb)
