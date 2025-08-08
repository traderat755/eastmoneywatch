import pandas as pd
import time
import os
from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka, get_latest_trade_date, is_trading_time
from datetime import datetime
from data_processor import apply_sorting
from services.pick_service import set_shared_picked_data
import logging

def worker(queue, interval=1, initial_concept_df=None, initial_changes_df=None, batch_interval=300, shared_picked_data=None):
    logging.debug("[worker_queue] 启动，推送到主进程Queue并定时批量写入磁盘")
    if shared_picked_data is not None:
        set_shared_picked_data(shared_picked_data)
        logging.debug(f"[worker_queue] set_shared_picked_data: id={id(shared_picked_data)}, keys={list(shared_picked_data.keys()) if shared_picked_data else 'None'}")

    # 设置静态目录
    static_dir = setup_static_directory()

    # 获取当前交易日
    current_date = get_latest_trade_date()
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
    logging.debug(f"[worker_queue] 当日changes文件路径: {changes_path}")

    # 定义标准字段
    standard_columns = [
        '股票代码', '时间', '名称', '相关信息', '类型', '板块名称', '四舍五入取整', '上下午', '时间排序', '标识'
    ]

    # 初始化master_df，使用从backend_service传递的初始数据
    logging.debug(f"[worker_queue] initial_changes_df类型: {type(initial_changes_df)}")
    if initial_changes_df is not None:
        # 如果是DataFrame
        if hasattr(initial_changes_df, 'empty'):
            is_empty = initial_changes_df.empty
            # 字段校验
            if not is_empty:
                df_columns = list(initial_changes_df.columns)
                if not all(col in df_columns for col in standard_columns):
                    logging.debug(f"[worker_queue] initial_changes_df字段不匹配，收到字段: {df_columns}")
                    logging.debug(f"[worker_queue] 内容预览: {initial_changes_df.head(2).to_dict('records') if hasattr(initial_changes_df, 'head') else str(initial_changes_df)[:200]}")
                    initial_changes_df = pd.DataFrame(columns=standard_columns)
                    is_empty = True
        elif isinstance(initial_changes_df, dict) or 'DictProxy' in str(type(initial_changes_df)):
            try:
                dict_data = dict(initial_changes_df)
                logging.debug(f"[worker_queue] initial_changes_df转dict内容预览: {str(dict_data)[:200]}")
                if 'records' in dict_data and dict_data['records']:
                    records = list(dict_data['records'])
                    logging.debug(f"[worker_queue] records长度: {len(records)}，前1条: {records[:1]}")
                    initial_changes_df = pd.DataFrame(records)
                    # 字段校验
                    df_columns = list(initial_changes_df.columns)
                    if not all(col in df_columns for col in standard_columns):
                        logging.debug(f"[worker_queue] initial_changes_df字段不匹配，收到字段: {df_columns}")
                        logging.debug(f"[worker_queue] 内容预览: {initial_changes_df.head(2).to_dict('records') if hasattr(initial_changes_df, 'head') else str(initial_changes_df)[:200]}")
                        initial_changes_df = pd.DataFrame(columns=standard_columns)
                        is_empty = True
                    else:
                        is_empty = initial_changes_df.empty
                else:
                    logging.debug(f"[worker_queue] dict_data无'records'或records为空，初始化为空DataFrame")
                    initial_changes_df = pd.DataFrame(columns=standard_columns)
                    is_empty = True
            except Exception as e:
                logging.debug(f"[worker_queue] initial_changes_df无法转为DataFrame: {e}")
                initial_changes_df = pd.DataFrame(columns=standard_columns)
                is_empty = True
        else:
            logging.debug(f"[worker_queue] 未知类型，初始化为空DataFrame")
            initial_changes_df = pd.DataFrame(columns=standard_columns)
            is_empty = True
        if not is_empty:
            master_df = initial_changes_df.copy()
            logging.debug(f"[worker_queue] 使用传递的changes数据初始化master_df，记录数: {len(master_df)}")
            # 应用排序
            if not master_df.empty:
                master_df = apply_sorting(master_df,initial_concept_df, for_frontend=False)
                logging.debug(f"[worker_queue] 已对传递的数据应用排序")
        else:
            logging.debug("[worker_queue] 传递的changes数据为空或字段不匹配，初始化为空DataFrame")
            master_df = pd.DataFrame(columns=standard_columns)
    else:
        logging.debug("[worker_queue] 未传递changes数据，初始化为空DataFrame")
        master_df = pd.DataFrame(columns=standard_columns)

    # 启动时，将现有数据推送到队列
    if not master_df.empty:
        logging.debug(f"[worker_queue] master_df.dtypes: {master_df.dtypes}")
        # 为前端格式化数据
        frontend_df = apply_sorting(master_df,initial_concept_df, for_frontend=True)
        logging.debug(f"[worker_queue] frontend_df.dtypes: {frontend_df.dtypes}")
        initial_data = {
            "columns": list(frontend_df.columns),
            "values": frontend_df.values.tolist()
        }
        queue.put(initial_data)
        logging.debug(f"[worker_queue] 已将初始数据推送到队列")
    else:
        logging.debug(f"[worker_queue] 初始数据为空，不推送到队列")


    last_write = time.time()
    last_date = current_date
    uplimit_cache = {}

    while True:

        now = time.time()

        # 简化交易日检测：直接获取涨停板数据的last_limit_up_time作为交易日
        try:
            logging.debug(f"[worker_queue] 获取涨停板数据检测交易日")
            uplimit_df = uplimit10jqka()
            if not uplimit_df.empty and 'last_limit_up_time' in uplimit_df.columns:
                # 获取任意一条记录的last_limit_up_time日期
                sample_date = uplimit_df['last_limit_up_time'].iloc[0]
                if pd.notna(sample_date):
                    # 处理时间戳格式
                    if isinstance(sample_date, (int, float)) or (isinstance(sample_date, str) and sample_date.isdigit()):
                        # 时间戳格式，转换为日期
                        timestamp = int(sample_date)
                        # 假设时间戳是秒级，如果是毫秒级需要除以1000
                        if timestamp > 9999999999:  # 毫秒级时间戳
                            timestamp = timestamp // 1000
                        current_date = datetime.fromtimestamp(timestamp).strftime('%Y%m%d')
                        logging.debug(f"[worker_queue] 时间戳 {sample_date} 转换为日期: {current_date}")
                    elif isinstance(sample_date, str):
                        # 字符串格式，尝试提取日期部分
                        current_date = sample_date.split(' ')[0]  # 取日期部分
                        logging.debug(f"[worker_queue] 字符串日期处理: {sample_date} -> {current_date}")
                    else:
                        # 其他格式，转换为字符串后处理
                        current_date = str(sample_date).split(' ')[0]
                        logging.debug(f"[worker_queue] 其他格式日期处理: {sample_date} -> {current_date}")

                    logging.debug(f"[worker_queue] 从涨停板数据获取交易日: {current_date}")

                    # 检查交易日是否变化
                    if current_date != last_date:
                        # 交易日变化，更新文件路径
                        changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
                        logging.debug(f"[worker_queue] 交易日变化: {last_date} -> {current_date}")
                        logging.debug(f"[worker_queue] 新的changes文件路径: {changes_path}")
                        last_date = current_date
                        # 清空主DataFrame，开始新的交易日
                        master_df = pd.DataFrame(columns=standard_columns)
                        last_write = time.time()
                        # 清空涨停板缓存
                        uplimit_cache = {}
                        logging.debug(f"[worker_queue] 已清空数据缓存，准备收集新交易日数据")

                    # 更新涨停板缓存
                    uplimit_cache = dict(zip(uplimit_df['name'].astype(str), uplimit_df['high_days']))
                    logging.debug(f"[worker_queue] 涨停板数据缓存更新完成，映射数量: {len(uplimit_cache)}")
                else:
                    logging.debug(f"[worker_queue] 涨停板数据last_limit_up_time为空，使用缓存交易日: {last_date}")
                    current_date = last_date
            else:
                logging.debug(f"[worker_queue] 涨停板数据为空或无last_limit_up_time字段，使用缓存交易日: {last_date}")
                current_date = last_date
        except Exception as e:
            logging.debug(f"[worker_queue] 获取涨停板数据失败: {e}，使用缓存交易日: {last_date}")
            current_date = last_date

        # 检查是否为交易日且在交易时间内
        if is_trading_time():
            logging.debug("[worker_queue] 当前为交易日且在交易时间内，开始获取数据")

            # 获取基础股票异动数据（不包含概念板块信息）
            df = getChanges()
            logging.debug(f"[worker_queue] getChanges返回数据列名: {list(df.columns) if not df.empty else '空DataFrame'}")
            if not df.empty:
                logging.debug(f"[worker_queue] getChanges返回数据类型值: {df['类型'].unique().tolist()}")
                logging.debug(f"[worker_queue] getChanges返回数据dtypes: {df.dtypes}")
        else:
            logging.debug("[worker_queue] 当前非交易日或不在交易时间内，跳过数据获取")
            df = pd.DataFrame()

        # 无论是否在交易时间内，都要推送数据到前端
        # 修改逻辑：如果有新数据则处理新数据，否则推送现有数据
        if not df.empty:
            master_df = master_df[master_df['四舍五入取整'] != 0]
            # 将新数据追加到主DataFrame
            master_df = pd.concat([master_df, df], ignore_index=True)

            # 为确保每个板块中的每只股票只保留最新的一条记录，
            master_df.drop_duplicates(subset=['类型', '股票代码'], keep='last', inplace=True, ignore_index=True)

            # 应用统一排序，保留完整数据用于存储
            master_df = apply_sorting(master_df, initial_concept_df, uplimit_cache,for_frontend=False)

            # 调试：检查排序后的数据结构
            logging.debug(f"[worker_queue] 排序后完整数据列名: {list(master_df.columns)}")
            if not master_df.empty:
                logging.debug(f"[worker_queue] 排序后数据示例: {master_df.iloc[0].to_dict()}")

            # 为前端准备格式化数据
            frontend_df = apply_sorting(master_df, initial_concept_df, uplimit_cache, for_frontend=True)
            logging.debug(f"[worker_queue] frontend_df.dtypes: {frontend_df.dtypes}")

            full_data = {
                "columns": list(frontend_df.columns),
                "values": frontend_df.values.tolist()
            }
            queue.put(full_data)
            logging.debug(f"[worker_queue] 已将格式化数据({len(frontend_df)}条)推送到队列")
        elif not master_df.empty:
            # 非交易时间但有历史数据时，也要推送现有数据到前端
            logging.debug(f"[worker_queue] 非交易时间，推送现有数据({len(master_df)}条)到前端")
            # 为前端准备格式化数据
            frontend_df = apply_sorting(master_df, initial_concept_df, uplimit_cache, for_frontend=True)

            full_data = {
                "columns": list(frontend_df.columns),
                "values": frontend_df.values.tolist()
            }
            queue.put(full_data)
            logging.debug(f"[worker_queue] 已将现有数据({len(frontend_df)}条)推送到队列")

        now = time.time()
        if (now - last_write) >= batch_interval:
            if not master_df.empty:
                try:
                    master_df.to_csv(changes_path, index=False)
                    logging.debug(f"[worker_queue] 批量写入 {len(master_df)} 条总记录到 {changes_path}")
                    last_write = now
                except Exception as e:
                    logging.debug(f"[worker_queue] 批量写入失败: {e}")


        time.sleep(interval)