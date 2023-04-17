import os
from dataclasses import dataclass
from typing import List

import numpy as np
from sqlalchemy import Column, DateTime, Numeric, Text, BigInteger, JSON, or_, and_, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

from naive_mm_analytics.common import generate_id
from sqlalchemy.ext.asyncio import async_sessionmaker

# Create declarative base
Base = declarative_base()

async_session_factory = None


async def get_async_session() -> AsyncSession:
    global async_session_factory
    if async_session_factory is None:
        async_engine = get_async_engine()
        async_session_factory = async_sessionmaker(bind=async_engine)
    return async_session_factory()


# def get_session() -> Session:
#     engine = get_engine()
#     session_factory = sessionmaker(bind=engine)
#     return session_factory()


def get_async_engine():
    username = os.getenv("DB_USER")
    database = os.getenv("DB_NAME")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")

    engine = create_async_engine(
        f"postgresql+asyncpg://{username}:{password}@{host}:{port}/{database}",
        pool_size=5, max_overflow=10
    )
    return engine


@dataclass
class ORDER(Base):
    __tablename__ = 'order'

    id = Column(BigInteger, primary_key=True, default=generate_id)
    stitch_id = Column(BigInteger, nullable=True, default=generate_id)
    pair = Column(Text, nullable=False)
    price = Column(Numeric, nullable=True)
    exchange = Column(Text, nullable=False)
    size = Column(Numeric, nullable=False)
    type = Column(Text, nullable=False)
    trade_side = Column(Text, nullable=False)
    status = Column(Text, nullable=False)
    exchange_order_id = Column(Text, nullable=False)
    transaction_hash = Column(Text, nullable=True)
    created_time = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    last_updated_time = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    input_amount = Column(Numeric, nullable=True)
    input_token = Column(Text, nullable=True)
    output_amount = Column(Numeric, nullable=True)
    output_token = Column(Text, nullable=True)
    average_fill_price = Column(Numeric, nullable=True)
    fee_info = Column(JSON, nullable=True)

    def to_numpy(self) -> np.ndarray:
        return np.asarray([
            self.id,
            self.stitch_id,
            self.pair,
            self.price,
            self.exchange,
            self.size,
            self.type,
            self.trade_side,
            self.status,
            self.exchange_order_id,
            self.transaction_hash,
            self.created_time,
            self.last_updated_time,
            self.input_amount,
            self.input_token,
            self.output_amount,
            self.output_token,
            self.average_fill_price,
            self.fee_info
        ])


async def fetch_created_orders_after_timestamp(created_timestamp: datetime) -> List[ORDER]:
    async with await get_async_session() as session:
        query = select(ORDER).where(and_(ORDER.created_time >= created_timestamp, ORDER.status == 'CREATED'))
        result = await session.execute(query)
        orders = result.fetchall()
        return orders
# Create the table if it doesn't exist
# Base.metadata.create_all(engine)

# Create a session
# session = Session()
