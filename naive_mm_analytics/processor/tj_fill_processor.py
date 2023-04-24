import asyncio
import logging
import os
from decimal import Decimal
from typing import Optional
from urllib.parse import urlencode

from naive_mm_analytics.common import TradeSide, SessionFactory, SnowtraceTokenTransactionData, OrderStatus
from naive_mm_analytics.constants import SNOWTRACE_API_KEY, SNOWTRACE_API_URL, USDT_ON_AVAX_CONTRACT_ADDRESS
from naive_mm_analytics.database_operations import ORDER, update_order_with_fill_data

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
        results = await asyncio.gather(buy_tj_task, repopulate_task)

        # There absolutely must be transaction information on here at this point. If there isn't, that implies data
        # source corruption on our Orders table or a rare situation of a Snowtrace fuck up.
        # This rare situation still needs graceful handling.
        fill_info: SnowtraceTokenTransactionData = self.fills_cache.get(txn_hash)

        # This is to handle the edge case of infinite recursion where a transaction failed before it interacted with
        # the smart contract and hence won't be a part of the transaction history of the smart contract.
        if fill_info is None:
            return

        # We need to extract all the fields here and then call the update_order method
        order_id = order.id
        # We are naively assuming status to be filled here. There are cases of contract execution revert or gas too low
        # but we need to check if those get persisted in orders db in first place
        status = OrderStatus.FILLED

        if order.trade_side == TradeSide.BUY:
            input_amount = Decimal(order.size)
            input_token = "USDT"
            output_amount = results[0]

            if output_amount is None:
                logger.error("Failed to fetch AVAX output value")
                average_fill_price = None
            else:
                average_fill_price = round(Decimal(input_amount / output_amount), 4)
            output_token = "AVAX"

        else:
            # Same rounding formula is used while placing the order so this isn't a loss of precision
            input_amount = round(Decimal(order.size / order.price), 4)
            input_token = "AVAX"
            output_amount = fill_info.value
            output_token = "USDT"
            average_fill_price = round(Decimal(output_amount / input_amount), 4)

        fee_info = {
            "gas": fill_info.gas,
            "gasPrice": fill_info.gas_price,
            "gasUsed": fill_info.gas_used,
            "cumulativeGasUsed": fill_info.cumulative_gas_used
        }
        await update_order_with_fill_data(order_id=order_id, status=status.name, input_amount=input_amount,
                                          input_token=input_token, output_amount=output_amount,
                                          output_token=output_token, average_fill_price=average_fill_price,
                                          fee_info=fee_info)
        # Purging the processed fill from fills cache
        del self.fills_cache[txn_hash]

    async def get_additional_buy_tj_fill_info(self, order: ORDER) -> Optional[Decimal]:
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
                    address = results[-1]['to']
                    value = Decimal(results[-1]['value'])
                    output_avax_amount = round(value / 10 ** 18, 4)
                    logger.info(f"BUY TJ - To Address {address} Value {output_avax_amount}")

                    if self.wallet_address != address:
                        logger.error("ABORT MISSION, last internal transaction to address did not match wallet address")
                        return None
                    return output_avax_amount
                else:
                    logger.error(f"Error: {response.status}")
                    return None

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
                    else:
                        logger.warning("Transaction not recorded in on-chain snowtrace data")
                        return
                    logger.info(f"Rows Fetched: {len(results)}. Cache size before update: {len(self.fills_cache)}")
                    self.fills_cache.update({item["hash"]: SnowtraceTokenTransactionData(item) for item in results})
                    logger.info(f"Cache size after update: {len(self.fills_cache)}")
                else:
                    print(f"Error: {response.status}")

        # This call basically works like pagination for us.
        if self.fills_cache.get(txn_hash) is None:
            await self.populate_cache(txn_hash)
