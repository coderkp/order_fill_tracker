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


class SnowtraceTokenTransactionData:
    def __init__(self, data_dict):
        self.block_number = data_dict.get("blockNumber")
        self.timestamp = data_dict.get("timeStamp")
        self.hash = data_dict.get("hash")
        self.nonce = data_dict.get("nonce")
        self.block_hash = data_dict.get("blockHash")
        self.from_address = data_dict.get("from")
        self.to_address = data_dict.get("to")
        self.contract_address = data_dict.get("contractAddress")
        self.value = data_dict.get("value")
        self.token_name = data_dict.get("tokenName")
        self.token_symbol = data_dict.get("tokenSymbol")
        self.token_decimal = data_dict.get("tokenDecimal")
        self.transaction_index = data_dict.get("transactionIndex")
        self.gas = data_dict.get("gas")
        self.gas_price = data_dict.get("gasPrice")
        self.gas_used = data_dict.get("gasUsed")
        self.cumulative_gas_used = data_dict.get("cumulativeGasUsed")
        self.input = data_dict.get("input")
        self.confirmations = data_dict.get("confirmations")


class OkxTransactionAbstract:
    def __init__(self, data_dict):
        self.order_id = data_dict.get("id")
        self.average_fill_price = data_dict.get("average")
        info = data_dict.get("info")
        self.fee = {
            "fee": info.get("fee"),
            "fee_token": info.get("feeCcy")
        }
        self.status = info.get("state")
        self.cost = data_dict.get("cost")
        self.filled_quantity = data_dict.get("filled")
        self.fill_time = info.get("fillTime")
