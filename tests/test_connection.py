"""BoilinData WebSocket client tests"""
import pytest
import pytest_asyncio
import asyncio
from pprint import pprint
from py_boilingdata import BoilingData

boiling = BoilingData()

##
## Unmocked tests against BoilingData
##


# We setup the session once and close it after all tests
@pytest_asyncio.fixture(scope="session", autouse=True)
async def manage_boiling_connection():
    await boiling.connect()
    yield None
    await boiling.close()


@pytest.mark.asyncio
async def test_local_query():
    """Simple SQL statement runs locally with embedded DuckDB"""
    data = await boiling.execute("SELECT 1;")
    assert data == [(1,)]


@pytest.mark.asyncio
async def test_local_tables():
    """We should have Boiling Data Catalog entries"""
    resp = await boiling.execute("SHOW TABLES;")
    assert resp == [
        ("demo_full",),
        ("taxi_locations",),
        ("taxi_locations_limited",),
    ]


@pytest.mark.asyncio
async def test_bd_query():
    """Small response from Boiling"""
    q = "SELECT first_name, email FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 1"
    data = await boiling.execute(q)  # awaits as lont as the result is available
    assert data == [{"email": "ajordan0@com.com", "first_name": "Amanda"}]


@pytest.mark.asyncio
async def test_bd_query_cb():
    """Small response from Boiling with callback"""
    global data
    data = None

    # Note: This depends on the used BD_USERNAME but also data sets shared
    def cb(resp):
        global data
        data = resp

    q = "SELECT email FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 1"
    await boiling.execute(q, cb)  # only awaits as long as the request is dispatched
    q = "SELECT first_name, email FROM parquet_scan('s3://boilingdata-demo/test.parquet') LIMIT 1"
    await boiling.execute(q, cb)  # only awaits as long as the request is dispatched
    await asyncio.sleep(5)  # let them race...
    assert data == [{"email": "ajordan0@com.com", "first_name": "Amanda"}] or data == [
        {"email": "ajordan0@com.com"}
    ]


# @pytest.mark.asyncio
# async def test_bd_query_large(snapshot):
#     """Bigger response from Boiling"""

#     # Note: This depends on the used BD_USERNAME but also data sets shared
#     def cb(resp):
#         snapshot.assert_match(str(resp), "test_bd_query_large")

#     q = "SELECT * FROM parquet_scan('s3://boilingdata-demo/demo.parquet') ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, PULocationID, DOLocationID LIMIT 200"
#     await boiling.execute(q, cb)
