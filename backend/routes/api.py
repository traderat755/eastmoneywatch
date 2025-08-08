from fastapi import APIRouter
from models import StockData
from services.backend_service import (
    start_get_concepts,
    queue_get_concepts,
    search_concepts,
    get_concept_sectors,
    get_stock_sectors
)
from services.pick_service import (
    get_picked_stocks,
    add_picked_stock,
    update_picked_stock,
    delete_picked_stock
)

router = APIRouter()


@router.post("/api/start_get_concepts")
def api_start_get_concepts():
    return start_get_concepts()


@router.post("/api/queue_get_concepts")
def api_queue_get_concepts():
    """将getConcepts任务加入队列执行一次"""
    return queue_get_concepts()


@router.get("/api/picked")
def api_get_picked_stocks():
    """获取选中的股票列表"""
    return get_picked_stocks()


@router.post("/api/picked")
def api_add_picked_stock(stock_data: StockData):
    """添加股票到精选列表"""
    import logging
    logging.debug(f"[api/picked] POST请求接收到的原始数据: {stock_data}")
    logging.debug(f"[api/picked] 数据类型: {type(stock_data)}")
    logging.debug(f"[api/picked] 数据字段: 股票代码={stock_data.股票代码}, 股票名称={stock_data.股票名称}, 板块代码={stock_data.板块代码}, 板块名称={stock_data.板块名称}")
    return add_picked_stock(stock_data)


@router.put("/api/picked/{stock_code}")
def api_update_picked_stock(stock_code: str, stock_data: StockData):
    """更新精选列表中的股票信息"""
    return update_picked_stock(stock_code, stock_data)


@router.delete("/api/picked/{stock_code}")
def api_delete_picked_stock(stock_code: str):
    """从精选列表中删除股票"""
    return delete_picked_stock(stock_code)


@router.get("/api/concepts/search")
def api_search_concepts(q: str = ""):
    """搜索concept_df中的股票"""
    return search_concepts(q)


@router.get("/api/concepts/sectors")
def api_get_concept_sectors():
    """获取concept_df中的所有板块"""
    return get_concept_sectors()


@router.get("/api/concepts/stock-sectors/{stock_code}")
def api_get_stock_sectors(stock_code: str):
    """获取指定股票在concept_df中对应的所有板块"""
    return get_stock_sectors(stock_code)