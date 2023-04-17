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

    # Gas for buy TJ transactions might actually need to be populated by Sell TJ transactions
    # itself
    # Okay so structure going forward. Check for the hash in the L1 Cache
    # if hash present great
    # if buy txn then one more api call to be made, if sale, none then.
    # if hash not present, trigger both refill and buy api call together for buy
    # For sale, just the refill.

    # Refill based on start block number and end block number is 999999 or whatever.
    # We store using transaction hash to support multi-accounts in the future and hence
    # as a result don't care so much about pagination either. Just take the last block number you
    # get in the response above and make a subsequent query if the hash is still not available.
    # Pass the hash in concern to the refill function for it to be able to retrigger this step

    # And that boys, wraps up buy and sell tj

    # For OKX, we need to persist the timestamp last seen.
    # We can in the same way make another query for OKX to go further on timestamp.
    # All this would honestly be resolved once we have a Redis integration
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
                    logger.info(f"BUY TJ - To Address {results[-1]['to']} Value {results[-1]['value']}")
                else:
                    print(f"Error: {response.status}")

    async def process_fill_sell_tj(self):
        pass

