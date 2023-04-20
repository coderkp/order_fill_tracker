import logging
import os
from typing import Optional

import ccxt.async_support as ccxt
from datetime import datetime, timezone

from naive_mm_analytics.common import SessionFactory, OkxTransactionAbstract, OrderStatus
from naive_mm_analytics.database_operations import ORDER, update_order_with_fill_data

logger = logging.getLogger(__name__)
session_factory = SessionFactory()


class OkxFillProcessor:

    def __init__(self):
        self.fills_cache = {}
        self.last_seen_timestamp = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        self.exchange = ccxt.okex({
            'apiKey': os.getenv("OKX_API_KEY"),
            'secret': os.getenv("OKX_SECRET"),
            'password': os.getenv("OKX_PASSPHRASE"),
            'options': {
                'defaultType': 'spot',  # or 'futures' for futures trading
            }
        })

    async def process_fill_info(self, order: ORDER):
        repopulate_task = None

        # Type sensitivity for id
        if self.fills_cache.get(order.id) is not None:
            logger.info(f"Cache Hit for Id: {order.id}.")
        else:
            logger.info(f"Cache Miss for Id: {order.id}. Triggering repopulate")
            await self.populate_cache(timestamp=None)

        fill_info: OkxTransactionAbstract = self.fills_cache.get(order.id)
        average_fill_price = fill_info.average_fill_price
        fee_info = fill_info.fee
        if fill_info.status == "filled":
            status = OrderStatus.FILLED
        else:
            status = OrderStatus.CREATED

        # Think of profitability analysis and whether we need the input token and amounts.
        await update_order_with_fill_data(order_id=order.id, status=status.name, input_amount=None,
                                          input_token=None, output_amount=None,
                                          output_token=None, average_fill_price=average_fill_price,
                                          fee_info=fee_info)
        # Todo: Purge from fills_cache. Check this on TJ side too

    async def populate_cache(self, timestamp: Optional[int]):
        new_orders = await self.exchange.fetch_closed_orders(symbol='AVAX/USDT', since=timestamp)
        logger.info(f"Rows Fetched: {len(new_orders)}. Cache size before update: {len(self.fills_cache)}")

        self.fills_cache.update({item["id"]: OkxTransactionAbstract(item) for item in new_orders})

        # Todo: Maybe recurse till you get an empty response. Need to think of termination conditions on snowtrace too
