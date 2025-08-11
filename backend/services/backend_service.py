import os
import sys
import pandas as pd
from multiprocessing import Process, Queue
import logging
import logging.handlers
import akshare as ak

from utils import get_resource_path, get_latest_trade_date, setup_static_directory
from services.pick_service import load_picked_data, get_shared_picked_data
# Import target functions that will be run in subprocesses
from concepts import getConcepts
from prepare import prepareChanges
from worker_queue import worker as changes_worker


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
            elif isinstance(value, (int, float)) and pd.isna(value):
                cleaned_record[key] = ''
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    return cleaned_records


# --- Child Process Logging Setup ---

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


def run_get_concepts(q: Queue, level, concepts_path=None, concept_stocks_path=None):
    """子进程任务：配置日志并运行 getConcepts。"""
    _setup_child_logging(q, level)
    try:
        logging.debug(f"Starting getConcepts task...{concepts_path}, {concept_stocks_path}")
    
        getConcepts(concepts_path, concept_stocks_path)

        logging.info("getConcepts task finished successfully.")
    except Exception as e:
        logging.error(f"getConcepts task failed: {e}", exc_info=True)


def run_prepare_changes(q: Queue, date_str: str, level):
    """子进程任务：配置日志并运行 prepareChanges。"""
    _setup_child_logging(q, level)
    try:
        logging.debug(f"Starting prepareChanges task for date: {date_str}...")
        prepareChanges(date_str)
        logging.info(f"prepareChanges for date {date_str} finished successfully.")
    except Exception as e:
        logging.error(f"prepareChanges for date {date_str} failed: {e}", exc_info=True)


# --- Global Variables ---

concept_df = None
get_concepts_proc = None
get_changes_proc = None
initialization_completed = False
log_queue: Queue = None  # Will be set by initialize_backend_services
log_level = logging.INFO # Default log level


def _get_concept_file_paths():
    """获取概念文件路径的辅助函数"""
    from utils import get_resource_path
    static_dir = get_resource_path("")
    if static_dir is None:
        # 如果 get_resource_path 返回 None，则回退到原来的逻辑
        static_dir = setup_static_directory()
    concepts_path = os.path.join(static_dir, "concepts.csv")
    concept_stocks_path = os.path.join(static_dir, "concept_stocks.csv")
    return static_dir, concepts_path, concept_stocks_path


def initialize_backend_services(buffer_queue: Queue, lq: Queue, level):
    """
    初始化后端服务，启动必要的子进程。
    这个函数现在接收一个专用的日志队列(lq)。
    """
    global concept_df, get_concepts_proc, get_changes_proc, initialization_completed, log_queue, log_level
    log_queue = lq  # Store the log queue globally for other functions to use
    log_level = level # Store the log level globally

    logging.debug("[sidecar] 开始初始化后端服务...")
    static_dir, concepts_path, concept_stocks_path = _get_concept_file_paths()
    if concepts_path is None or not os.path.exists(concepts_path):
        try:
            logging.debug("[sidecar] 缺少 concepts.csv，启动 getConcepts 子进程...")
            
            # Calculate paths directly instead of relying on get_resource_path in subprocess
            os.makedirs(static_dir, exist_ok=True)
            
            logging.debug(f"[sidecar] 将使用路径: {concepts_path} 和 {concept_stocks_path}")
            
            get_concepts_proc = Process(target=run_get_concepts, args=(log_queue, log_level, concepts_path, concept_stocks_path), daemon=True)
            get_concepts_proc.start()
            
            logging.debug("[sidecar] 等待 getConcepts 完成...")
            get_concepts_proc.join()  # Wait for the process to complete
            
            if get_concepts_proc.exitcode == 0:
                logging.debug("[sidecar] getConcepts 执行成功")
                # No need to re-check with get_resource_path since we already know the path
                if not os.path.exists(concepts_path):
                    raise RuntimeError(f"getConcepts执行成功但文件不存在: {concepts_path}")
            else:
                logging.error(f"[sidecar] getConcepts 执行失败，退出码: {get_concepts_proc.exitcode}")
                raise RuntimeError("getConcepts failed to execute.")
        except Exception as e:
            logging.error(f"[sidecar] 自动调用 getConcepts 失败: {e}", exc_info=True)
            raise
    else:
        logging.debug("[sidecar] 检测到已存在 concepts.csv，跳过自动调用 getConcepts")

    try:
        if concepts_path is None or not os.path.exists(concepts_path):
            raise RuntimeError("concepts.csv 文件不存在，服务无法启动")
        
        dtype_dict = {'股票代码': str, '板块代码': str}
        concepts_df_part1 = pd.read_csv(concepts_path, dtype={'板块代码': str, '板块名称': str})
        concept_stocks_path = get_resource_path("concept_stocks.csv")
        if concept_stocks_path is None or not os.path.exists(concept_stocks_path):
            raise RuntimeError("concept_stocks.csv 文件不存在，服务无法启动")
        
        concept_stocks_df = pd.read_csv(concept_stocks_path, dtype=dtype_dict)
        concept_df = pd.merge(concept_stocks_df, concepts_df_part1, on='板块代码', how='left')
        concept_df = concept_df[['板块代码', '板块名称', '股票代码']]
        logging.debug(f"[sidecar] Concepts data loaded and merged. Total records: {len(concept_df)}")
        if concept_df.empty:
            raise RuntimeError("Concepts data is empty after merge.")
    except Exception as e:
        logging.error(f"[sidecar] Error loading concepts data: {e}", exc_info=True)
        raise

    try:
        static_dir, _, _ = _get_concept_file_paths()
        current_date = get_latest_trade_date()
        changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")

        if not os.path.exists(changes_path):
            logging.debug(f"[sidecar] 当天的changes文件不存在，开始运行 prepareChanges...")
            prepare_proc = Process(target=run_prepare_changes, args=(log_queue, current_date, log_level), daemon=True)
            prepare_proc.start()
            logging.debug(f"[sidecar] 已启动 prepareChanges 子进程，PID: {prepare_proc.pid}")
            prepare_proc.join() # Wait for completion
            if prepare_proc.exitcode == 0:
                logging.debug("[sidecar] prepareChanges 执行成功")
            else:
                logging.error(f"[sidecar] prepareChanges 执行失败，退出码: {prepare_proc.exitcode}")
        else:
            logging.debug(f"[sidecar] 检测到当天的changes文件已存在，跳过prepareChanges")
    except Exception as e:
        logging.error(f"[sidecar] 检查或运行 prepareChanges 失败: {e}", exc_info=True)

    # 加载picked数据并确保共享内存完全初始化
    load_picked_data(static_dir)
    
    # 强制同步数据到共享内存并验证
    from services.pick_service import force_sync_to_shared_memory, get_shared_picked_data, shared_picked_manager
    
    # 强制同步确保数据完整传递
    record_count = force_sync_to_shared_memory()
    logging.debug(f"[sidecar] 强制同步完成，共享内存中有 {record_count} 条记录")
    
    # 获取共享内存引用
    shared_picked_data_ref = get_shared_picked_data()
    shared_picked_manager_ref = shared_picked_manager
    
    # 验证共享内存状态
    if shared_picked_data_ref is not None and 'records' in shared_picked_data_ref:
        logging.debug(f"[sidecar] 共享内存验证成功，records数量: {len(shared_picked_data_ref['records'])}")
    else:
        logging.error("[sidecar] 共享内存验证失败，worker进程可能无法访问picked数据")

    if get_changes_proc is None or not get_changes_proc.is_alive():
        try:
            current_changes_df = None
            if os.path.exists(changes_path):
                logging.debug(f"[sidecar] 读取当前changes文件传递给worker: {changes_path}")
                current_changes_df = pd.read_csv(changes_path)
                current_changes_df = current_changes_df.fillna('').infer_objects(copy=False)
                current_changes_df['板块名称'] = '未知板块'
                standard_columns = ['股票代码', '板块名称', '板块代码', '时间', '名称', '相关信息', '类型', '四舍五入取整', '上下午','标识']
                if not all(col in current_changes_df.columns for col in standard_columns):
                    logging.warning(f"[backend_service] changes文件字段{current_changes_df.columns}不匹配，将使用空DataFrame。")
                    current_changes_df = pd.DataFrame(columns=standard_columns)
            
            # 注意：worker_queue.worker也需要修改以接受log_queue作为第一个参数
            get_changes_proc = Process(
                target=changes_worker,
                args=(
                    log_queue,  # 1. Pass the log queue to the worker
                    log_level,
                    buffer_queue,
                    2,
                    concept_df.copy() if concept_df is not None else None,
                    current_changes_df.copy() if current_changes_df is not None else None,
                    300,
                    shared_picked_data_ref  # 传递共享数据引用
                ),
                daemon=True
            )
            get_changes_proc.start()
            logging.debug(f"[sidecar] 已启动 worker_queue 子进程, PID: {get_changes_proc.pid}")
        except Exception as e:
            logging.error(f"[sidecar] 启动 worker_queue 子进程失败: {e}", exc_info=True)
    else:
        logging.debug("[sidecar] worker_queue 子进程已在运行，跳过启动")

    initialization_completed = True
    logging.debug("[sidecar] 后端服务初始化完成")


def start_get_concepts():
    """启动获取概念数据的子进程"""
    global get_concepts_proc, log_queue, log_level
    if log_queue is None:
        return {"status": "error", "message": "服务尚未初始化"}
    
    if get_concepts_proc is not None and get_concepts_proc.is_alive():
        return {"status": "already running", "pid": get_concepts_proc.pid}

    # For manual start, use default paths
    static_dir, concepts_path, concept_stocks_path = _get_concept_file_paths()
    
    get_concepts_proc = Process(target=run_get_concepts, args=(log_queue, log_level, concepts_path, concept_stocks_path), daemon=True)
    get_concepts_proc.start()
    logging.info(f"Started getConcepts process with PID: {get_concepts_proc.pid}")
    return {"status": "started", "pid": get_concepts_proc.pid}


def queue_get_concepts():
    """将getConcepts任务放入队列执行一次"""
    global log_queue, log_level
    if log_queue is None:
        return {"status": "error", "message": "服务尚未初始化"}
        
    try:
        # For queued execution, use default paths
        static_dir, concepts_path, concept_stocks_path = _get_concept_file_paths()
        
        proc = Process(target=run_get_concepts, args=(log_queue, log_level, concepts_path, concept_stocks_path), daemon=True)
        proc.start()
        logging.info(f"[queue_get_concepts] 已将getConcepts任务加入队列执行，PID: {proc.pid}")
        return {"status": "queued", "pid": proc.pid, "message": "getConcepts任务已加入队列执行"}
    except Exception as e:
        logging.error(f"[queue_get_concepts] 执行getConcepts任务失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def search_concepts(query):
    """搜索concept_df中的股票，直接从文件读取以获取完整数据"""
    try:
        if not query:
            return {"status": "success", "data": []}

        concepts_path = get_resource_path("concepts.csv")
        concept_stocks_path = get_resource_path("concept_stocks.csv")
        if concepts_path is None or concept_stocks_path is None:
            return {"status": "error", "message": "概念数据文件路径未找到"}
        
        if not os.path.exists(concepts_path) or not os.path.exists(concept_stocks_path):
            return {"status": "error", "message": "概念数据文件不存在"}

        concepts_df_part = pd.read_csv(concepts_path)
        concept_stocks_df = pd.read_csv(concept_stocks_path, dtype={'板块代码': str, '股票代码': str})
        original_concept_df = pd.merge(concept_stocks_df, concepts_df_part, on='板块代码', how='left')
        original_concept_df.dropna(subset=['板块名称'], inplace=True)

        logging.debug(f"[api/concepts/search] 查询: '{query}'")
        exact_match_df = original_concept_df[original_concept_df['股票代码'].astype(str) == query]

        if not exact_match_df.empty:
            results = exact_match_df[['股票代码', '板块代码', '板块名称']]
            logging.debug(f"[api/concepts/search] 精确匹配到 {len(results)} 条记录")
        else:
            filtered_df = original_concept_df[
                original_concept_df['股票代码'].astype(str).str.contains(query, na=False)
            ]
            results = filtered_df[['股票代码', '板块代码', '板块名称']].head(50)
            logging.debug(f"[api/concepts/search] 模糊搜索到 {len(results)} 条记录")

        results = results.fillna('')
        stock_name = "未知"
        try:
            if len(query) == 6:
                prefix = 'sz' if not query.startswith(('6', '8')) else 'sh' if query.startswith('6') else 'bj'
                symbol = prefix + query
                stock_info_df = ak.stock_individual_info_em(symbol=query)
                # Find the value where item is '股票简称'
                name_series = stock_info_df[stock_info_df['item'] == '股票简称']['value']
                if not name_series.empty:
                    stock_name = name_series.iloc[0]
                    logging.debug(f"[search_concepts] 获取到股票名称: {stock_name}")
                results['股票名称'] = stock_name
        except Exception as e:
            logging.warning(f"[search_concepts] 获取股票名称失败: {e}")
            results['股票名称'] = stock_name


        records = results.to_dict('records')
        records = clean_nan_values(records)
        return {"status": "success", "data": records}
    except Exception as e:
        logging.error(f"[api/concepts/search] 搜索失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def get_concept_sectors():
    """获取concept_df中的所有板块"""
    global concept_df
    if concept_df is None or concept_df.empty:
        return {"status": "error", "message": "概念数据未加载"}
    try:
        sectors = concept_df[['板块代码', '板块名称']].drop_duplicates()
        sectors = sectors.fillna('').infer_objects(copy=False)
        records = sectors.to_dict('records')
        records = clean_nan_values(records)
        logging.debug(f"[api/concepts/sectors] 获取板块列表，共{len(records)}个板块")
        return {"status": "success", "data": records}
    except Exception as e:
        logging.error(f"[api/concepts/sectors] 获取板块列表失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def get_stock_sectors(stock_code):
    """获取指定股票在concept_df中对应的所有板块"""
    global concept_df
    if concept_df is None or concept_df.empty:
        return {"status": "error", "message": "概念数据未加载"}
    try:
        stock_sectors = concept_df[concept_df['股票代码'] == stock_code][['板块代码', '板块名称']].drop_duplicates()
        stock_sectors = stock_sectors.fillna('').infer_objects(copy=False)
        records = stock_sectors.to_dict('records')
        records = clean_nan_values(records)
        logging.debug(f"[api/concepts/stock-sectors] 获取股票{stock_code}的板块列表，共{len(records)}个板块")
        return {"status": "success", "data": records}
    except Exception as e:
        logging.error(f"[api/concepts/stock-sectors] 获取股票板块列表失败: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


def get_watch_status(watch_process):
    """Get the status of the fluctuation watch process"""
    if watch_process is None:
        return {"status": "not_running"}
    if watch_process.is_alive():
        return {"status": "running", "pid": watch_process.pid}
    else:
        return {"status": "stopped", "return_code": watch_process.exitcode}