from pydantic import BaseModel


class StockData(BaseModel):
    股票代码: str
    股票名称: str
    板块代码: str
    板块名称: str