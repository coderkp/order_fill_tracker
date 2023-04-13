import logging
from datetime import datetime, timezone

import numpy as np
import asyncio

# Define the maximum number of rows in the rolling data store
from naive_mm_analytics.database_operations import fetch_created_orders_after_timestamp, ORDER

max_rows = 1000

# Initialize the rolling data store as an empty NumPy structured array
#dtype = [('col1', int), ('col2', float), ('col3', str)]

order_dtype = np.dtype({
    "names": [field.name for field in ORDER.__dataclass_fields__.values()],
    "formats": [np.dtype(field.type) for field in ORDER.__dataclass_fields__.values()]
})

rolling_data = np.empty(max_rows, dtype=order_dtype)

# Initialize the index of the first row and the number of rows in the data store
first_row = 0
num_rows = 0

# This is a default timestamp of Jan 1st 2023, long before we started developing this system
last_seen_timestamp = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
# Create a semaphore with a maximum of 5 permits
semaphore = asyncio.Semaphore(5)
logger = logging.getLogger(__name__)


async def process_order(order):
    # Process the order here
    # ...

    # Get the max and min values of a column named 'price'
    async with semaphore:
        max_price = np.max(rolling_data['price'][first_row:num_rows])
        min_price = np.min(rolling_data['price'][first_row:num_rows])
        # ...


async def main():
    global last_seen_timestamp, rolling_data, first_row, num_rows
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    while True:
        # Get new orders that haven't been processed yet TODO: Pass some sort of timestamp here
        new_orders = await fetch_created_orders_after_timestamp(last_seen_timestamp)

        # Add new orders to the rolling data store
        num_new_rows = len(new_orders)
        if num_rows + num_new_rows > max_rows:
            # If adding new rows would exceed the maximum size, remove the oldest rows
            logger.critical("Rolling Buffer Full: Increase size or throughput")
            num_remove_rows = num_rows + num_new_rows - max_rows
            rolling_data = np.delete(rolling_data, slice(first_row, first_row + num_remove_rows))
            first_row += num_remove_rows
            num_rows = max_rows - num_new_rows

        ''' Currently, we can simply use the time of the last row, because our orders table is append only and rows
        are updated only when they reach a final state. For a model, where there are intermediate states, i.e. things
        like limit orders with partial fills, we would start to use the last_updated_time field than created field
        and process atomic updates to an order using the partially filled status. This use-case is precisely why we
        go with numpy array because ops like min, max or any other ops are generally a bit easier with numpy. This is 
        a bit debatable, but I didn't have the time to do any deeper of a dive than this. 
        '''
        last_seen_timestamp = rolling_data[-1]['created_time']
        rolling_data[num_rows:num_rows + num_new_rows] = np.array(new_orders, dtype=order_dtype)
        num_rows += num_new_rows

        # Process orders from the beginning of the rolling data store
        for i in range(first_row, num_rows):
            order = rolling_data[i]
            asyncio.create_task(process_order(order))

        # Wait for all tasks to complete before fetching new orders again
        await asyncio.sleep(0)


if __name__ == '__main__':
    asyncio.run(main())
