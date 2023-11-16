"""BoilinData WebSocket client tests"""
import pytest
from py_boilingdata import BoilingData


@pytest.mark.asyncio
async def test_connection():
    """Send simple SQL and wait for 4 messages back"""
    boiling = BoilingData()
    await boiling.connect()  # get_boiling_websocket_connection(credentials)
    await boiling.send('{"sql":"SELECT 42;","requestId":"pythonWebSocketTest1"}')
    counter = 0
    while counter < 4:
        counter = counter + 1
        response = await boiling.recv()
        print(response)
    await boiling.close()
    assert True
