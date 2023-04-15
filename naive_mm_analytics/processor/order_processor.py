import logging
from collections import deque
from datetime import datetime, timezone

import numpy as np
import asyncio

# Define the maximum number of rows in the rolling data store
from naive_mm_analytics.database_operations import fetch_created_orders_after_timestamp, ORDER

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

    async def process_order(self, order):
        # Get the max and min values of a column named 'price'
        async with self.semaphore:
            pass

            # Depending on which branch the order belongs to, run that processing logic.
            # build branch processing logic.

    async def start(self):

        while True:
            # Get new orders that haven't been processed yet
            new_orders = await fetch_created_orders_after_timestamp(self.last_seen_timestamp)

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

            # Need to explicitly imply self.rolling_data returns order for clarity
            self.last_seen_timestamp = self.rolling_data[-1].created_time

            # Process orders from the beginning of the rolling data store
            # I am aware accessing deque by position is not optimal but at 10 elements a cycle
            # we are still firmly under Constant Time complexity
            while len(self.rolling_data) > 0:
                # Todo: The 10 below should be configurable
                tasks = [asyncio.create_task(self.process_order(self.rolling_data[j])) for j in
                         range(0, min(10, len(self.rolling_data)))]
                # Wait for the tasks to complete
                await asyncio.gather(*tasks)

            # At this point we know the orders were processed. Ideally we should be reading errors from the
            # gathered tasks and letting those orders be. Currently, we will drop the updates for those

            # Just chill for a bit and have a coffee.
            await asyncio.sleep(90)
