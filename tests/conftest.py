import asyncio
import pytest
from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
