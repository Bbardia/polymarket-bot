import asyncio
import os

import pytest

from src.v3.api import UnifiedPolymarketAPI

pytestmark = pytest.mark.skipif(
    os.getenv("POLYMARKET_RUN_METERED_TESTS") != "1",
    reason="set POLYMARKET_RUN_METERED_TESTS=1 for explicit public-API checks",
)


def test_official_public_sdk_lists_an_active_market():
    async def check():
        api = UnifiedPolymarketAPI()
        pages = []
        async for page in api.public_client.list_markets(closed=False, page_size=1):
            pages.append(page)
            break
        assert len(pages) == 1
        assert pages[0].items
        market = pages[0].items[0]
        assert str(market.id)
        assert market.outcomes is not None
        assert not api.secure_client_initialized

    asyncio.run(check())
