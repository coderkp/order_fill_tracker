import time
from enum import Enum

import aiohttp as aiohttp


class OrderType(Enum):
    MARKET = 1
    LIMIT = 2
    LIMIT_MAKER = 3

    def is_limit_type(self):
        return self in (OrderType.LIMIT, OrderType.LIMIT_MAKER)


class TradeSide(Enum):
    BUY = 1
    SELL = 2

    @classmethod
    def from_string(cls, string_input):
        try:
            return cls.__members__[string_input]
        except KeyError:
            raise ValueError(f"{string_input} is not a valid trade side")

class OrderStatus(Enum):
    CREATED = 1
    CANCELLED = 2
    FILLED = 3


class Exchange(Enum):
    TRADER_JOE = 1
    OKX = 2

    @classmethod
    def from_string(cls, string_input):
        try:
            return cls.__members__[string_input]
        except KeyError:
            raise ValueError(f"{string_input} is not a valid exchange")

def generate_id():
    # Ideally we shouldn't have a situation of an id collision but if we get there we can implement locking here or
    # switch to truncating uuid to 32 characters or lesser.
    return int(time.time() * 1e9)


class SessionFactory:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        if self.session is None:
            self.session = aiohttp.ClientSession()
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        if self.session is not None:
            await self.session.close()
            self.session = None
