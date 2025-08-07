import os
import sys
import pandas as pd
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


# Global variable for picked data
picked_df = None


def load_picked_data():
    """加载picked.csv到内存中"""
    global picked_df
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
        else:
            print("[pick_service] 未发现picked.csv文件，创建空的picked_df", flush=True)
            picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

    except Exception as e:
        print(f"[pick_service] 处理picked.csv时出错: {e}", flush=True)
        picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])


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
            return {"status": "error", "message": "股票已存在于精选列表中"}

        # 添加新股票到内存DataFrame
        new_stock = pd.DataFrame([stock_dict])
        picked_df = pd.concat([picked_df, new_stock], ignore_index=True)

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

        print(f"[api/picked] 添加股票成功: {stock_dict['股票名称']}", flush=True)
        return {"status": "success", "message": "股票添加成功"}

    except Exception as e:
        print(f"[api/picked] 添加股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def update_picked_stock(stock_code, stock_data):
    """更新精选列表中的股票信息"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            return {"status": "error", "message": "精选列表为空"}

        # 查找股票
        stock_index = picked_df[picked_df['股票代码'] == stock_code].index
        if len(stock_index) == 0:
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

        print(f"[api/picked] 更新股票成功: {stock_code}", flush=True)
        return {"status": "success", "message": "股票更新成功"}

    except Exception as e:
        print(f"[api/picked] 更新股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def delete_picked_stock(stock_code):
    """从精选列表中删除股票"""
    global picked_df
    try:
        if picked_df is None or picked_df.empty:
            return {"status": "error", "message": "精选列表为空"}

        # 检查股票是否存在
        if stock_code not in picked_df['股票代码'].values:
            return {"status": "error", "message": "股票不存在于精选列表中"}

        # 删除股票
        picked_df = picked_df[picked_df['股票代码'] != stock_code]

        # 同步保存到文件
        picked_path = get_resource_path("static/picked.csv")
        if picked_path:
            picked_df.to_csv(picked_path, index=False, encoding='utf-8')

        print(f"[api/picked] 删除股票成功: {stock_code}", flush=True)
        return {"status": "success", "message": "股票删除成功"}

    except Exception as e:
        print(f"[api/picked] 删除股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def get_current_picked_df():
    """获取当前内存中的picked_df，供worker进程使用"""
    global picked_df
    return picked_df