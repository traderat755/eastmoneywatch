from pydantic import BaseModel, Field


class StockData(BaseModel):
    股票代码: str = Field(..., description="股票代码")
    股票名称: str = Field("", description="股票名称")  # 设为可选，默认为空字符串
    板块代码: str = Field(..., description="板块代码")
    板块名称: str = Field(..., description="板块名称")