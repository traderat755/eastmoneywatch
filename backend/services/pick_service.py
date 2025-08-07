import os
import sys
import pandas as pd
from multiprocessing import Manager
from utils import get_resource_path, get_data_dir


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


def load_picked_data():
    """加载picked.csv到内存和共享内存中"""
    global picked_df, shared_picked_data

    # 初始化共享内存
    init_shared_picked_data()
    try:
        picked_path = get_resource_path("static/picked.csv")
        if picked_path and os.path.exists(picked_path):
            print(f"[pick_service] 发现picked.csv文件: {picked_path}", flush=True)
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)

            # 清理NaN值
            picked_df = picked_df.fillna('')

            print(f"[pick_service] 加载picked.csv到内存成功，共{len(picked_df)}条记录", flush=True)
            print(f"[pick_service] picked.csv字段: {list(picked_df.columns)}", flush=True)
            print(f"[pick_service] picked.csv股票代码列数据类型: {picked_df['股票代码'].dtype}", flush=True)

            # 同步到共享内存
            _sync_to_shared_memory()
        else:
            print("[pick_service] 未发现picked.csv文件，创建空的picked_df", flush=True)
            picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

            # 同步到共享内存
            _sync_to_shared_memory()

    except Exception as e:
        print(f"[pick_service] 处理picked.csv时出错: {e}", flush=True)
        picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

        # 同步到共享内存
        _sync_to_shared_memory()


def get_picked_stocks():
    """获取选中的股票列表"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            print("[api/picked] picked_df为空", flush=True)
            return {"status": "success", "data": []}

        print(f"[api/picked] 获取选中股票列表，共{len(picked_df)}条记录", flush=True)

        # 转换为字典并清理NaN值
        records = picked_df.to_dict('records')
        records = clean_nan_values(records)

        print(f"[api/picked] 返回数据记录数: {len(records)}", flush=True)
        return {"status": "success", "data": records}

    except Exception as e:
        print(f"[api/picked] 获取选中股票列表失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def add_picked_stock(stock_data):
    """添加股票到精选列表"""
    global picked_df
    try:
        # 转换为字典
        stock_dict = stock_data.dict()
        # 检查是否已存在
        if not picked_df.empty and stock_dict['股票代码'] in picked_df['股票代码'].values:
            print(f"[api/picked] 添加股票失败: 股票已存在于精选列表中", flush=True)
            print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
            print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
            return {"status": "error", "message": "股票已存在于精选列表中"}
        # 添加新股票到内存DataFrame
        new_stock = pd.DataFrame([stock_dict])
        picked_df = pd.concat([new_stock,picked_df], ignore_index=True)
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
        print(f"[api/picked] 添加股票成功: {stock_dict['股票名称']}", flush=True)
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "success", "message": "股票添加成功"}
    except Exception as e:
        print(f"[api/picked] 添加股票失败: {e}", flush=True)
        _sync_to_shared_memory()
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "error", "message": str(e)}


def update_picked_stock(stock_code, stock_data):
    """更新精选列表中的股票信息"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            print(f"[api/picked] 更新股票失败: 精选列表为空", flush=True)
            print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
            print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
            return {"status": "error", "message": "精选列表为空"}
        # 查找股票
        stock_index = picked_df[picked_df['股票代码'] == stock_code].index
        if len(stock_index) == 0:
            print(f"[api/picked] 更新股票失败: 股票不存在于精选列表中", flush=True)
            print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
            print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
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
        print(f"[api/picked] 更新股票成功: {stock_code}", flush=True)
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "success", "message": "股票更新成功"}
    except Exception as e:
        print(f"[api/picked] 更新股票失败: {e}", flush=True)
        _sync_to_shared_memory()
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "error", "message": str(e)}


def delete_picked_stock(stock_code_or_sector):
    """从精选列表中删除股票或整个板块"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            print(f"[api/picked] 删除失败: 精选列表为空", flush=True)
            print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
            print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
            return {"status": "error", "message": "精选列表为空"}
        # 判断是股票代码还是板块名称
        if stock_code_or_sector in picked_df['板块名称'].values:
            # 按板块名称批量删除
            print(f"[api/picked] 批量删除板块: {stock_code_or_sector}", flush=True)
            picked_df = picked_df[picked_df['板块名称'] != stock_code_or_sector]
            msg = f"板块 {stock_code_or_sector} 已删除精选"
        elif stock_code_or_sector in picked_df['股票代码'].values:
            # 按股票代码删除
            print(f"[api/picked] 删除股票: {stock_code_or_sector}", flush=True)
            picked_df = picked_df[picked_df['股票代码'] != stock_code_or_sector]
            msg = f"股票 {stock_code_or_sector} 已删除精选"
        else:
            print(f"[api/picked] 删除失败: 未找到 {stock_code_or_sector}", flush=True)
            return {"status": "error", "message": f"未找到 {stock_code_or_sector}"}
        # 同步保存到文件
        picked_path = get_resource_path("static/picked.csv")
        if picked_path:
            picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        # 同步到共享内存
        _sync_to_shared_memory()
        print(f"[api/picked] 删除成功: {stock_code_or_sector}", flush=True)
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "success", "message": msg}
    except Exception as e:
        print(f"[api/picked] 删除失败: {e}", flush=True)
        _sync_to_shared_memory()
        print(f"[api/picked] 操作后 picked_df 长度: {len(picked_df) if picked_df is not None else 'None'}", flush=True)
        print(f"[api/picked] 操作后 shared_picked_data keys: {list(shared_picked_data.keys()) if shared_picked_data else 'None'}", flush=True)
        return {"status": "error", "message": str(e)}


def _sync_to_shared_memory():
    """将picked_df同步到共享内存"""
    global picked_df, shared_picked_data
    print(f"[_sync_to_shared_memory] called, id(shared_picked_data)={id(shared_picked_data)}, picked_df is None? {picked_df is None}", flush=True)
    if shared_picked_data is not None and picked_df is not None:
        try:
            # 清空共享列表
            shared_picked_data['records'][:] = []
            # 将DataFrame转换为字典列表并添加到共享内存
            records = picked_df.to_dict('records')
            for record in records:
                shared_picked_data['records'].append(record)
            print(f"[_sync_to_shared_memory] 已同步{len(records)}条记录到共享内存, id(shared_picked_data)={id(shared_picked_data)}", flush=True)
        except Exception as e:
            print(f"[_sync_to_shared_memory] 同步到共享内存失败: {e}", flush=True)
    else:
        print(f"[_sync_to_shared_memory] shared_picked_data或picked_df为None，无法同步", flush=True)


def get_shared_picked_data():
    """获取共享内存中的picked数据，供外部进程使用"""
    global shared_picked_data
    return shared_picked_data


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
            print(f"[get_shared_picked_df] 共享内存 records 长度: {len(records)} 示例: {records[0] if records else '无'}", flush=True)
            if records:
                df = pd.DataFrame(records)
                return df.fillna('')
            else:
                return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])
        except Exception as e:
            print(f"[pick_service] 从共享内存构建DataFrame失败: {e}", flush=True)
            return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])
    else:
        print(f"[get_shared_picked_df] shared_picked_data is None or 'records' 不在其中, type={type(shared_picked_data)}, keys={list(shared_picked_data.keys()) if hasattr(shared_picked_data, 'keys') else '无'}", flush=True)
        return pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])


def set_shared_picked_data(data):
    """设置全局 shared_picked_data，供worker进程调用"""
    global shared_picked_data
    shared_picked_data = data
    print(f"[set_shared_picked_data] 设置 shared_picked_data: type={type(shared_picked_data)}, id={id(shared_picked_data)}, keys={list(shared_picked_data.keys()) if hasattr(shared_picked_data, 'keys') else '无'}", flush=True)
    # 自动补全 records 字段
    if shared_picked_data is not None and 'records' not in shared_picked_data:
        try:
            from multiprocessing import Manager
            if isinstance(shared_picked_data, dict):
                shared_picked_data['records'] = []
            else:
                # 兼容 Manager().dict()
                shared_picked_data['records'] = Manager().list()
            print(f"[set_shared_picked_data] 自动补全 records 字段", flush=True)
        except Exception as e:
            print(f"[set_shared_picked_data] 自动补全 records 字段失败: {e}", flush=True)