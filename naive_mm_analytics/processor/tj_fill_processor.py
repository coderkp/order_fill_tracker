import logging
import os
from urllib.parse import urlencode

from naive_mm_analytics.common import TradeSide, SessionFactory
from naive_mm_analytics.constants import SNOWTRACE_API_KEY, SNOWTRACE_API_URL
from naive_mm_analytics.database_operations import ORDER

logger = logging.getLogger(__name__)
session_factory = SessionFactory()

class TjFillProcessor:

    def __init__(self):

        self.wallet_address = os.getenv("TJ_WALLET_ADDRESS")

    async def process_fill_info(self, order: ORDER):
        if order.trade_side == TradeSide.BUY:
            await self.process_fill_buy_tj()
        elif order.trade_side == TradeSide.SELL:
            await self.process_fill_sell_tj()
        else:
            logger.error("Invalid Trade Side")

    async def process_fill_buy_tj(self, order: ORDER):
        async with session_factory as session:
            params = {
                'module': 'account',
                'action': 'txlistinternal',
                'txhash': order.transaction_hash,
                'apikey': SNOWTRACE_API_KEY
            }

            query_string = urlencode(params)
            request_url = f"{SNOWTRACE_API_URL}?{query_string}"
            async with session.get(request_url) as response:
                if response.status == 200:
                    response_data = await response.json()
                    # process the response data here
                    results = response_data["result"]

                else:
                    print(f"Error: {response.status}")

    async def process_fill_sell_tj(self):
        pass

