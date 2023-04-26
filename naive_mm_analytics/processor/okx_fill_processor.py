import asyncio
import logging
import os
from asyncio import Lock
from decimal import Decimal

import ccxt.async_support as ccxt

from naive_mm_analytics.common import SessionFactory, OkxTransactionAbstract, OrderStatus
from naive_mm_analytics.database_operations import ORDER, update_order_with_fill_data

logger = logging.getLogger(__name__)
session_factory = SessionFactory()


class OkxFillProcessor:

    def __init__(self):
        self.fills_cache = {}
        # This has to be in epoch
        self.last_seen_timestamp = 0
        self.exchange = ccxt.okex({
            'apiKey': os.getenv("OKX_API_KEY"),
            'secret': os.getenv("OKX_SECRET"),
            'password': os.getenv("OKX_PASSPHRASE"),
            'options': {
                'defaultType': 'spot',  # or 'futures' for futures trading
            }
        })
        self.populate_cache_task = None

    async def process_fill_info(self, order: ORDER):

        # Todo: Consider adding better logic to reject older orders
        if (order.created_time.timestamp() * 1000) < 1681765123000:
            logger.info("Too Old an order to process")
            return

        if self.fills_cache.get(order.exchange_order_id) is not None:
            logger.info(f"Cache Hit for Exchg Id: {order.exchange_order_id}")
        else:
            logger.info(f"Cache Miss for Exch Id: {order.exchange_order_id}. Triggering repopulate")
            if self.populate_cache_task is None or self.populate_cache_task.done():
                # create a new populate_cache task
                self.populate_cache_task = asyncio.create_task(self.populate_cache(order.exchange_order_id))
            await self.populate_cache_task


        fill_info: OkxTransactionAbstract = self.fills_cache.get(order.exchange_order_id)

        # This is to handle the situation of old data in the Orders table for which Okx might not return a response
        if fill_info is None:
            logger.warning("OKX Fill Processor - Fill Info is Empty")
            # logger.info(f"Fills cache {self.fills_cache}")
            # await asyncio.sleep(150)
            return

        average_fill_price: Decimal = round(fill_info.average_fill_price, 4)
        fee_info = fill_info.fee
        if fill_info.status == "filled":
            status = OrderStatus.FILLED
        else:
            status = OrderStatus.CREATED

        logger.info(f"Next line calls DB")
        # Todo: Think of profitability analysis and whether we need the input token and amounts.
        await update_order_with_fill_data(order_id=order.id, status=status.name, input_amount=None,
                                          input_token=None, output_amount=None,
                                          output_token=None, average_fill_price=average_fill_price,
                                          fee_info=fee_info)

        # Purging the processed fill from fills cache
        # IF last seen timestamp is ever maintained in Redis, this is where it should be updated as
        # if a server crashes, seen but unprocessed rows will again become unseen
        # del self.fills_cache[order.exchange_order_id]

    async def populate_cache(self, exchange_id: str):
        new_orders = await self.exchange.fetch_closed_orders(symbol='AVAX/USDT', since=self.last_seen_timestamp)
        logger.info(f"Okx Rows Fetched: {len(new_orders)}. Cache size before update: {len(self.fills_cache)}")

        if len(new_orders) == 0:
            logger.info("No New Orders on Okx")
            return

        for item in new_orders:
            transaction_abstract = OkxTransactionAbstract(item)
            self.last_seen_timestamp = max(self.last_seen_timestamp, transaction_abstract.timestamp + 1)
            self.fills_cache.update({item["id"]: transaction_abstract})

        print(exchange_id)
        if self.fills_cache.get(exchange_id) is None:
            await self.populate_cache(exchange_id)
        logger.info(f"Pop cache trigger by {exchange_id} completed")
