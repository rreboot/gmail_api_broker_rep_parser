import dataclasses
from dateutil import parser
from datetime import datetime
import re


@dataclasses.dataclass
class TableRecord:

    def __post_init__(self):
        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            type = field.type

            if isinstance(value, str) and type in (float, int):
                value = re.sub(r'\s+', '', value).replace(',', '.')
                try:
                    setattr(self, field.name, field.type(float(value)))
                except Exception as e:
                    raise e

            if isinstance(value, str) and type == datetime:
                try:
                    setattr(self, field.name, parser.parse(value, fuzzy=True))
                except Exception as e:
                    raise e

    def toTuple(self):
        return dataclasses.astuple(self)

    @property
    def fields(self):
        return [field.name for field in dataclasses.fields(self)]


@dataclasses.dataclass
class PortfolioTableRecord(TableRecord):
    name: str
    ISIN: str
    market_price_cur: str
    quantity_start: int
    value_start: float
    market_price_start: float
    market_price_start_wonkd: float
    nkd_start: float
    quantity_end: int
    value_end: float
    market_price_end: float
    market_price_end_wonkd: float
    nkd_end: float
    amount_period: int
    market_val_period: float
    cred_for_trans: int
    writeoffs_for_trans: int
    outgoing_balance: int
    period_from: datetime
    period_to: datetime


if __name__ == '__main__':
    record = PortfolioTableRecord(
        'name1', 'isin1', 'market_price_cur1',
        0.0, 1, '2.0', '3', '4,0', '5  0,0  ', 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        datetime.now(), '30.03.2020')
