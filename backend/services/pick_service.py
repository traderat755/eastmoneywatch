import os
import sys
import pandas as pd
from multiprocessing import Manager
from utils import get_resource_path, get_data_dir
import logging

def clean_nan_values(records):
    """清理字典列表中的NaN值，确保JSON兼容"""
    if not records:
        return records

    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            if pd.isna(value):
                cleaned_record[key] = ''
            elif isinstance(value, (int, float)) and (pd.isna(value) or value != value):  # 检查NaN
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)

    return cleaned_records


# Global variables for picked data
picked_df = None
shared_picked_manager = None
shared_picked_data = None


def init_shared_picked_data():
    """初始化共享内存管理器"""
    global shared_picked_manager, shared_picked_data
    if shared_picked_manager is None:
        shared_picked_manager = Manager()
        shared_picked_data = shared_picked_manager.dict()
        # 初始化空的共享数据结构
        shared_picked_data['records'] = shared_picked_manager.list()


def load_picked_data(static_dir=None):
    """加载picked.csv到内存和共享内存中"""
    global picked_df, shared_picked_data

    # 初始化共享内存
    init_shared_picked_data()
    try:
        if static_dir is None:
            picked_path = get_resource_path("static/picked.csv")
        else:
            picked_path = os.path.join(static_dir, "picked.csv")
        
        if picked_path and os.path.exists(picked_path):
            logging.debug(f"[pick_service] 发现picked.csv文件: {picked_path}")
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)

            # 清理NaN值
            picked_df = picked_df.fillna('').infer_objects(copy=False)

            logging.debug(f"[pick_service] 加载picked.csv到内存成功，共{len(picked_df)}条记录")
            logging.debug(f"[pick_service] picked.csv字段: {list(picked_df.columns)}")
            logging.debug(f"[pick_service] picked.csv股票代码列数据类型: {picked_df['股票代码'].dtype}")

            # 同步到共享内存
            _sync_to_shared_memory()
        else:
            logging.debug("[pick_service] 未发现picked.csv文件，创建空的picked_df")
            picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

            # 同步到共享内存
            _sync_to_shared_memory()

    except Exception as e:
        logging.debug(f"[pick_service] 处理picked.csv时出错: {e}")
        picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

        # 同步到共享内存
        _sync_to_shared_memory()


def get_picked_stocks():
    """获取选中的股票列表"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            logging.debug("[api/picked] picked_df为空")
            return {"status": "success", "data": []}

        logging.debug(f"[api/picked] 获取选中股票列表，共{len(picked_df)}条记录")

        # 转换为字典并清理NaN值
        records = picked_df.to_dict('records')
        records = clean_nan_values(records)

        logging.debug(f"[api/picked] 返回数据记录数: {len(records)}")
        return {"status": "success", "data": records}

    except Exception as e:
        logging.debug(f"[api/picked] 获取选中股票列表失败: {e}")
        return {"status": "error", "message": str(e)}


def add_picked_stock(stock_data):
    """添加股票到精选列表"""
    global picked_df
    try:
        # 转换为字典
        stock_dict = stock_data.dict()
        logging.debug(f"[api/picked] 添加股票入参: {stock_dict}")
        
        # 确保股票名称字段存在，如果为空则使用股票代码
        if not stock_dict.get('股票名称'):
            stock_dict['股票名称'] = stock_dict.get('股票代码', '')
            logging.debug(f"[api/picked] 股票名称为空，使用股票代码: {stock_dict['股票名称']}")
        
        # 检查是否已存在
        if not picked_df.empty and stock_dict['股票代码'] in picked_df['股票代码'].values:
            logging.debug(f"[api/picked] 股票已存在，进行覆盖更新: {stock_dict['股票代码']}")
            # 找到现有股票的索引
            stock_index = picked_df[picked_df['股票代码'] == stock_dict['股票代码']].index[0]
            # 更新现有股票信息
            for key, value in stock_dict.items():
                if key in picked_df.columns:
                    picked_df.loc[stock_index, key] = value
            logging.debug(f"[api/picked] 覆盖更新股票成功: {stock_dict['股票名称']}")
        else:
            logging.debug(f"[api/picked] 股票不存在，添加新股票: {stock_dict['股票代码']}")
            # 添加新股票到内存DataFrame
            new_stock = pd.DataFrame([stock_dict])
            picked_df = pd.concat([new_stock, picked_df], ignore_index=True)
            logging.debug(f"[api/picked] 添加新股票成功: {stock_dict['股票名称']}")
        
        # 同步保存到文件
        picked_path = get_resource_path("static/picked.csv")
        if not picked_path:
            # 如果文件不存在，创建目录并设置路径
            if hasattr(sys, '_MEIPASS'):
                static_dir = os.path.join(get_data_dir(), "static")
            else:
                static_dir = "static"
            os.makedirs(static_dir, exist_ok=True)
            picked_path = os.path.join(static_dir, "picked.csv")
        picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        
        # 同步到共享内存
        _sync_to_shared_memory()
        
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "success", "message": "股票添加/更新成功"}
    except Exception as e:
        logging.debug(f"[api/picked] 添加股票失败: {e}")
        _sync_to_shared_memory()
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "error", "message": str(e)}


def update_picked_stock(stock_code, stock_data):
    """更新精选列表中的股票信息"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            logging.debug(f"[api/picked] 更新股票失败: 精选列表为空")
            logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
            logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
            return {"status": "error", "message": "精选列表为空"}
        # 查找股票
        stock_index = picked_df[picked_df['股票代码'] == stock_code].index
        if len(stock_index) == 0:
            logging.debug(f"[api/picked] 更新股票失败: 股票不存在于精选列表中")
            logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
            logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
            return {"status": "error", "message": "股票不存在于精选列表中"}
        # 更新股票信息
        stock_dict = stock_data.dict()
        for key, value in stock_dict.items():
            if key in picked_df.columns:
                picked_df.loc[stock_index[0], key] = value
        # 同步保存到文件
        picked_path = get_resource_path("static/picked.csv")
        if picked_path:
            picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        # 同步到共享内存
        _sync_to_shared_memory()
        logging.debug(f"[api/picked] 更新股票成功: {stock_code}")
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "success", "message": "股票更新成功"}
    except Exception as e:
        logging.debug(f"[api/picked] 更新股票失败: {e}")
        _sync_to_shared_memory()
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "error", "message": str(e)}


def delete_picked_stock(stock_code_or_sector):
    """从精选列表中删除股票或整个板块"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            logging.debug(f"[api/picked] 删除失败: 精选列表为空")
            logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
            logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
            return {"status": "error", "message": "精选列表为空"}
        # 判断是股票代码还是板块名称
        if stock_code_or_sector in picked_df['板块名称'].values:
            # 按板块名称批量删除
            logging.debug(f"[api/picked] 批量删除板块: {stock_code_or_sector}")
            picked_df = picked_df[picked_df['板块名称'] != stock_code_or_sector]
            msg = f"板块 {stock_code_or_sector} 已删除精选"
        elif stock_code_or_sector in picked_df['股票代码'].values:
            # 按股票代码删除
            logging.debug(f"[api/picked] 删除股票: {stock_code_or_sector}")
            picked_df = picked_df[picked_df['股票代码'] != stock_code_or_sector]
            msg = f"股票 {stock_code_or_sector} 已删除精选"
        else:
            logging.debug(f"[api/picked] 删除失败: 未找到 {stock_code_or_sector}")
            return {"status": "error", "message": f"未找到 {stock_code_or_sector}"}
        # 同步保存到文件
        picked_path = get_resource_path("static/picked.csv")
        if picked_path:
            picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        # 同步到共享内存
        _sync_to_shared_memory()
        logging.debug(f"[api/picked] 删除成功: {stock_code_or_sector}")
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "success", "message": msg}
    except Exception as e:
        logging.debug(f"[api/picked] 删除失败: {e}")
        _sync_to_shared_memory()
        logging.debug(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}")
        logging.debug(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}")
        return {"status": "error", "message": str(e)}


def _sync_to_shared_memory():
    """将picked_df同步到共享内存"""
    global picked_df, shared_picked_data
    logging.debug(f"[_sync_to_shared_memory] called, id(shared_picked_data)={id(shared_picked_data)}, picked_df is None? {picked_df is None}")
    if shared_picked_data is not None and picked_df is not None:
        try:
            # 清空共享列表
            shared_picked_data['records'][:] = []
            # 将DataFrame转换为字典列表并添加到共享内存
            records = picked_df.to_dict('records')
            for record in records:
                shared_picked_data['records'].append(record)
            logging.debug(f"[_sync_to_shared_memory] 已同步{len(records)}条记录到共享内存, id(shared_picked_data)={id(shared_picked_data)}")
        except Exception as e:
            logging.debug(f"[_sync_to_shared_memory] 同步到共享内存失败: {e}")
    else:
        logging.debug(f"[_sync_to_shared_memory] shared_picked_data或picked_df为None，无法同步")


def get_shared_picked_data():
    """获取共享内存中的picked数据，供外部进程使用"""
    global shared_picked_data
    return shared_picked_data


def get_shared_picked_manager():
    """获取共享内存管理器，供外部进程使用"""
    global shared_picked_manager
    return shared_picked_manager


def get_current_picked_df():
    """获取当前内存中的picked_df，供worker进程使用"""
    global picked_df
    return picked_df


def get_shared_picked_df():
    """从共享内存构建picked_df，供worker进程使用"""
    global shared_picked_data
    if shared_picked_data is not None and 'records' in shared_picked_data:
        try:
            records = list(shared_picked_data['records'])
            logging.debug(f"[get_shared_picked_df] 共享内存 records 长度: {len(records)} 示例: {records[0] if records else '无'}")
            if records:
                df = pd.DataFrame(records)
                return df.fillna('').infer_objects(copy=False)
            else:
                return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])
        except Exception as e:
            logging.debug(f"[pick_service] 从共享内存构建DataFrame失败: {e}")
            return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])
    else:
        logging.debug(f"[get_shared_picked_df] shared_picked_data is None or 'records' 不在其中, type={type(shared_picked_data)}, keys={list(shared_picked_data.keys()) if hasattr(shared_picked_data, 'keys') else '无'}")
        return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])


def force_sync_to_shared_memory():
    """强制将当前picked_df同步到共享内存，用于初始化时确保数据同步"""
    global picked_df, shared_picked_data
    logging.debug(f"[force_sync_to_shared_memory] 强制同步开始, picked_df长度: {len(picked_df) if picked_df is not None else 'None'}")
    
    # 确保共享内存已初始化
    if shared_picked_data is None:
        init_shared_picked_data()
    
    # 强制同步数据
    _sync_to_shared_memory()
    
    # 验证同步结果
    if shared_picked_data is not None and 'records' in shared_picked_data:
        record_count = len(shared_picked_data['records'])
        logging.debug(f"[force_sync_to_shared_memory] 强制同步完成, 共享内存records数量: {record_count}")
        return record_count
    else:
        logging.error("[force_sync_to_shared_memory] 强制同步失败，共享内存仍为空")
        return 0


def set_shared_picked_data(data):
    """设置全局 shared_picked_data，供worker进程调用"""
    global shared_picked_data, shared_picked_manager
    shared_picked_data = data
    logging.debug(f"[set_shared_picked_data] 设置 shared_picked_data: type={type(shared_picked_data)}, id={id(shared_picked_data)}, keys={list(shared_picked_data.keys()) if hasattr(shared_picked_data, 'keys') else '无'}")
    
    # 验证数据完整性
    if shared_picked_data is not None:
        if 'records' not in shared_picked_data:
            logging.warning("[set_shared_picked_data] 共享内存中缺少records字段，尝试补全")
            try:
                from multiprocessing import Manager
                if isinstance(shared_picked_data, dict):
                    shared_picked_data['records'] = []
                else:
                    # 兼容 Manager().dict()
                    shared_picked_data['records'] = Manager().list()
                logging.debug("[set_shared_picked_data] 自动补全 records 字段成功")
            except Exception as e:
                logging.error(f"[set_shared_picked_data] 自动补全 records 字段失败: {e}")
        else:
            # 验证records字段的数据
            records_count = len(shared_picked_data['records']) if 'records' in shared_picked_data else 0
            logging.debug(f"[set_shared_picked_data] 共享内存records字段包含 {records_count} 条记录")
            if records_count > 0:
                logging.debug(f"[set_shared_picked_data] 首条记录示例: {shared_picked_data['records'][0]}")
    else:
        logging.warning("[set_shared_picked_data] 接收到None的共享数据，这可能导致picked功能异常")