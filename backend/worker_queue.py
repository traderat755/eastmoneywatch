import pandas as pd
import time
import os
import logging
import logging.handlers
from multiprocessing import Queue
from datetime import datetime

from fluctuation import getChanges
from utils import setup_static_directory, uplimit10jqka, get_latest_trade_date, is_trading_time
from data_processor import apply_sorting
from services.pick_service import set_shared_picked_data


def _setup_child_logging(q: Queue, level):
    """在子进程中配置logging，将日志记录发送到队列。"""
    if q is None:
        # Fallback for safety, though it shouldn't happen in normal operation
        logging.basicConfig(level=logging.DEBUG)
        logging.warning("Log queue not provided to child process.")
        return

    # 创建一个handler，它将日志写入队列
    queue_handler = logging.handlers.QueueHandler(q)
    
    # 获取根logger，并设置其level
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # 清除所有现有的handlers，只使用QueueHandler
    logger.handlers = [queue_handler]


def worker(log_q: Queue, log_level, data_q: Queue, interval=5, initial_concept_df=None, initial_changes_df=None, batch_interval=300, shared_picked_data=None):
    """
    这个worker进程现在接收两个队列：
    - log_q: 用于发送日志记录到主进程。
    - data_q: 用于发送处理好的数据到主进程。
    """
    # 第一件事：设置此子进程的日志记录
    _setup_child_logging(log_q, log_level)

    logging.debug("[worker_queue] 启动，日志已重定向到队列。")
    
    if shared_picked_data is not None:
        set_shared_picked_data(shared_picked_data)
        logging.debug(f"[worker_queue] set_shared_picked_data: id={id(shared_picked_data)}, keys={list(shared_picked_data.keys()) if shared_picked_data else 'None'}")

    static_dir = setup_static_directory()
    current_date = get_latest_trade_date()
    changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")

    standard_columns = [
        '股票代码', '时间', '名称', '相关信息', '类型', '板块名称', '四舍五入取整', '上下午', '时间排序', '标识'
    ]

    master_df = initial_changes_df if initial_changes_df is not None else pd.DataFrame(columns=standard_columns)
    last_write = time.time()
    last_date = current_date
    uplimit_cache = {}

    while True:
        try:
            current_trade_day = get_latest_trade_date()
            if current_trade_day != last_date:
                logging.info(f"[worker_queue] 检测到新的交易日: {last_date} -> {current_trade_day}")
                last_date = current_trade_day
                current_date = current_trade_day
                changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
                master_df = pd.DataFrame(columns=standard_columns)
                last_write = time.time()
                uplimit_cache = {}
                logging.info(f"[worker_queue] 已重置数据缓存，新的存储路径: {changes_path}")

            df = pd.DataFrame()
            if is_trading_time():
                logging.debug("[worker_queue] 交易时间内，开始获取数据...")
                df = getChanges()
                if not df.empty:
                    logging.debug(f"[worker_queue] getChanges 返回 {len(df)} 条记录")
                else:
                    logging.debug("[worker_queue] getChanges 未返回新数据")
                
                # 更新涨停数据缓存
                uplimit_df = uplimit10jqka()
                if not uplimit_df.empty:
                    uplimit_cache = dict(zip(uplimit_df['name'].astype(str), uplimit_df['high_days']))
                    logging.debug(f"[worker_queue] 涨停数据缓存更新，数量: {len(uplimit_cache)}")
            else:
                logging.debug("[worker_queue] 非交易时间，跳过数据获取")

            if not df.empty:
                master_df = pd.concat([master_df, df], ignore_index=True)
                master_df.drop_duplicates(subset=['类型', '股票代码'], keep='last', inplace=True, ignore_index=True)

            # 仅在有数据时进行处理和推送
            if not master_df.empty:
                master_df = master_df[master_df['四舍五入取整'] != 0]
                
                # 应用排序和板块信息
                processed_df = apply_sorting(master_df, initial_concept_df, uplimit_cache, for_frontend=False)
                frontend_df = apply_sorting(processed_df, initial_concept_df, uplimit_cache, for_frontend=True)

                full_data = {
                    "columns": list(frontend_df.columns),
                    "values": frontend_df.values.tolist()
                }
                data_q.put(full_data)
                logging.debug(f"[worker_queue] 已将 {len(frontend_df)} 条格式化数据推送到数据队列")

                now = time.time()
                if (now - last_write) >= batch_interval:
                    try:
                        processed_df.to_csv(changes_path, index=False)
                        logging.info(f"[worker_queue] 批量写入 {len(processed_df)} 条记录到 {changes_path}")
                        last_write = now
                    except Exception as e:
                        logging.error(f"[worker_queue] 批量写入磁盘失败: {e}", exc_info=True)
            else:
                logging.debug("[worker_queue] master_df 为空，无需处理或推送")

        except Exception as e:
            logging.error(f"[worker_queue] worker主循环发生错误: {e}", exc_info=True)
        
        time.sleep(interval)
