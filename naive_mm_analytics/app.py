import asyncio
import logging

from naive_mm_analytics.database_operations import calculate_arb_performance
from naive_mm_analytics.processor.order_processor import OrderProcessor


async def main():
    order_processor = OrderProcessor()
    order_task = asyncio.create_task(order_processor.start())
    await asyncio.gather(order_task)
    #await calculate_arb_performance()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    asyncio.run(main())
