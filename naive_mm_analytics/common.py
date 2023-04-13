import time
from enum import Enum


class OrderType(Enum):
    MARKET = 1
    LIMIT = 2
    LIMIT_MAKER = 3

    def is_limit_type(self):
        return self in (OrderType.LIMIT, OrderType.LIMIT_MAKER)


class TradeSide(Enum):
    BUY = 1
    SELL = 2


class OrderStatus(Enum):
    CREATED = 1
    CANCELLED = 2
    FILLED = 3


class Exchange(Enum):
    TRADER_JOE = 1
    OKX = 2


def generate_id():
    # Ideally we shouldn't have a situation of an id collision but if we get there we can implement locking here or
    # switch to truncating uuid to 32 characters or lesser.
    return int(time.time() * 1e9)
