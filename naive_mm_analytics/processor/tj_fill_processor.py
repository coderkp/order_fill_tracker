import asyncio
import logging
import os
from urllib.parse import urlencode

from naive_mm_analytics.common import TradeSide, SessionFactory, SnowtraceTokenTransactionData
from naive_mm_analytics.constants import SNOWTRACE_API_KEY, SNOWTRACE_API_URL, USDT_ON_AVAX_CONTRACT_ADDRESS
from naive_mm_analytics.database_operations import ORDER

logger = logging.getLogger(__name__)
session_factory = SessionFactory()

# A separate fills cache cleaner needs to be written that runs once in say 30 mins and goes through
# the fills cache and the orders table by transaction hash. For orders mark filled, stop populating!
class TjFillProcessor:

    def __init__(self):

        self.wallet_address = os.getenv("TJ_WALLET_ADDRESS")
        self.fills_cache = {}
        self.last_seen_block = 0

    async def process_fill_info(self, order: ORDER):

        txn_hash = order.transaction_hash
        repopulate_task = None
        buy_tj_task = None
        if self.fills_cache.get(txn_hash) is not None:
            logger.info(f"Cache Hit for Hash: {txn_hash}.")
        else:
            logger.info(f"Cache Miss for Hash: {txn_hash}. Triggering re-populate")
            repopulate_task = asyncio.create_task(self.populate_cache(txn_hash))

        if order.trade_side == TradeSide.BUY:
            buy_tj_task = asyncio.create_task(self.get_additional_buy_tj_fill_info(order))

        '''
        Context: We need to make an additional API call when the TradeSide of an Order is buy. This call may or may
        not coincide with the cache repopulation. If it does, then both API calls would run effectively in parallel. 
        For a sale trade_side not requiring repopulation, the below statement would effectively be 
        asyncio.gather(None, None) and hence won't block anything.
        '''
        await asyncio.gather(buy_tj_task, repopulate_task)

        # There absolutely must be transaction information on here at this point. If there isn't, that implies data
        # source corruption on our Orders table or a rare situation of a Snowtrace fuck up.
        # This rare situation still needs graceful handling.
        fill_info: SnowtraceTokenTransactionData = self.fills_cache.get(txn_hash)

        # Okay let's get the fill structured and processed below
        # We need to extract all the fields here and then call the update_order method

    # Gas for buy TJ transactions might actually need to be populated by Sell TJ transactions
    # itself

    # For OKX, we need to persist the timestamp last seen.
    # We can in the same way make another query for OKX to go further on timestamp.
    # All this would honestly be resolved once we have a Redis integration
    async def get_additional_buy_tj_fill_info(self, order: ORDER):
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

    async def populate_cache(self, txn_hash: str):
        async with session_factory as session:
            params = {
                'module': 'account',
                'action': 'tokentx',
                'contractaddress': USDT_ON_AVAX_CONTRACT_ADDRESS,
                'address': self.wallet_address,
                'startblock': str(self.last_seen_block),
                'endblock': '99999999',
                'sort': 'asc',
                'apikey': SNOWTRACE_API_KEY
            }

            query_string = urlencode(params)
            request_url = f"{SNOWTRACE_API_URL}?{query_string}"
            async with session.get(request_url) as response:
                if response.status == 200:
                    response_data = await response.json()
                    # process the response data here
                    results = response_data["result"]
                    if len(results) > 0:
                        self.last_seen_block = results[-1].get("blockNumber")
                    logger.info(f"Rows Fetched: {len(results)}. Cache size before update: {len(self.fills_cache)}")
                    self.fills_cache.update({item["hash"]: SnowtraceTokenTransactionData(item) for item in results})
                    logger.info(f"Cache size after update: {len(self.fills_cache)}")
                else:
                    print(f"Error: {response.status}")

        # This call basically works like pagination for us.
        if self.fills_cache.get(txn_hash) is None:
            await self.populate_cache(txn_hash)

