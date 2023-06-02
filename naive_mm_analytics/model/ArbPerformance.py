from sqlalchemy import Table, Column, BigInteger, Numeric, String, MetaData
from sqlalchemy.orm import registry

metadata = MetaData()
mapper_registry = registry()

arb_performance_table = Table('arb_performance', metadata,
    Column('stitch_id', BigInteger, primary_key=True),
    Column('pair', String),
    Column('profit', Numeric)
)


class ArbPerformance:
    def __init__(self, stitch_id, pair, profit):
        self.stitch_id = stitch_id
        self.pair = pair
        self.profit = profit


mapper_registry.map_imperatively(ArbPerformance, arb_performance_table)
