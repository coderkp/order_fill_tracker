import logging
from collections import deque
from datetime import datetime, timezone
import asyncio

# Define the maximum number of rows in the rolling data store
from naive_mm_analytics.common import Exchange, TradeSide
from naive_mm_analytics.database_operations import fetch_created_orders_after_timestamp, ORDER
from naive_mm_analytics.processor.okx_fill_processor import OkxFillProcessor
from naive_mm_analytics.processor.tj_fill_processor import TjFillProcessor

logger = logging.getLogger(__name__)


class OrderProcessor:

    def __init__(self):
        self.max_rows = 1000

        # self.order_dtype = np.dtype({
        #     "names": [field.name for field in ORDER.__dataclass_fields__.values()],
        #     "formats": [np.dtype(field.type) for field in ORDER.__dataclass_fields__.values()]
        # })

        # Max Rows can be made configurable
        self.max_rows = 1000
        self.rolling_data = deque(maxlen=self.max_rows)

        # This is a default timestamp of Jan 1st 2023, long before we started developing this system
        self.last_seen_timestamp = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        # Create a semaphore with a maximum of 5 permits
        self.semaphore = asyncio.Semaphore(5)
        self.okx_fill_processor = OkxFillProcessor()
        self.tj_fill_processor = TjFillProcessor()

    async def process_order(self, order: ORDER):
        logger.info("In process_order")
        async with self.semaphore:
            # Todo: Remove Log
            logger.info(f"Semaphore value {self.semaphore._value}")
            exchange = Exchange.from_string(order.exchange)

            if exchange == Exchange.OKX:
                logger.info("OKX Identified")
                await self.okx_fill_processor.process_fill_info(order)
            elif exchange == Exchange.TRADER_JOE:
                logger.info("TraderJoe Identified")
                await self.tj_fill_processor.process_fill_info(order)
            else:
                logger.warning("Unknown Exchange")
            logger.info(f"Finally the order {order}")

    async def fetch_orders_from_db(self):
        logger.info("In fetch orders from db")
        while True:
            new_orders = await fetch_created_orders_after_timestamp(self.last_seen_timestamp)

            logger.info(f"New orders length {len(new_orders)}")
            # Add new orders to the rolling data store
            num_new_rows = len(new_orders)
            rolling_data_size = len(self.rolling_data)
            if rolling_data_size + num_new_rows > self.max_rows:
                logger.critical("Rolling Buffer Full: Increase size or throughput")
                open_slots = self.max_rows - rolling_data_size
                # Truncating the newly pulled rows to read only those that we can add to the queue
                new_orders = new_orders[:open_slots]

            # New rows have been added to the rolling data
            self.rolling_data.extend(new_orders)
            logger.info(f"Rolling data length {len(self.rolling_data)}")
            # Need to explicitly imply self.rolling_data returns order for clarity
            self.last_seen_timestamp = self.rolling_data[-1].ORDER.created_time
            print(f"LAST seen timestamp {self.last_seen_timestamp} and tye {type(self.last_seen_timestamp)}")
            # Just chill for a bit and have a coffee.
            await asyncio.sleep(120)

    async def process_orders(self):
        logger.info("In process_ordersss")
        while True:
            # To avoid blocking this method going crazy when there are no orders to process
            if len(self.rolling_data) == 0:
                await asyncio.sleep(10)

            # Process orders from the beginning of the rolling data store
            # I am aware accessing deque by position is not optimal but at 10 elements a cycle
            # we are still firmly under Constant Time complexity
            # Todo: The 10 below should be configurable
            tasks_len = min(10, len(self.rolling_data))
            tasks = [asyncio.create_task(self.process_order(self.rolling_data[j].ORDER)) for j in
                     range(0, tasks_len)]
            # Wait for the tasks to complete
            await asyncio.gather(*tasks)

            # For this naive model of insertion and deletion to work, there absolutely CANNOT be inserts at the
            # front of the queue. The insert and process parts work in different coroutines and hence the app code
            # needs to make sure it modifies the data structure while maintaining its integrity.
            # A potential future optimization would be finding a way to block inserts at the front.
            for i in range(0, tasks_len):
                popped_order: ORDER = self.rolling_data.pop().ORDER
                logger.info(f"Order {popped_order.id} has been processed")
            # At this point we know the orders were processed. Ideally we should be reading errors from the
            # gathered tasks and letting those orders be. Currently, we will drop the updates for those

    async def start(self):
        db_fetch_task = asyncio.create_task(self.fetch_orders_from_db())
        process_order_task = asyncio.create_task(self.process_orders())
        logger.info("Triggered DB fetch and process orders")
        await asyncio.gather(db_fetch_task, process_order_task)

