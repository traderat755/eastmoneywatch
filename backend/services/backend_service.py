import os
import sys
import subprocess
import pandas as pd
from multiprocessing import Process
from utils import get_resource_path
from services.pick_service import load_picked_data, get_shared_picked_data
import logging
import akshare as ak

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


# Global variables
concept_df = None
get_concepts_proc = None
get_changes_proc = None
initialization_completed = False


def initialize_backend_services(buffer_queue):
    """初始化后端服务，启动必要的子进程"""
    global concept_df, get_concepts_proc, get_changes_proc, initialization_completed

    logging.debug("[sidecar] 开始初始化后端服务...")

    # Check if concepts.csv exists before calling getConcepts
    concepts_path = get_resource_path("static/concepts.csv")
    getConcepts_started = False
    if not concepts_path or not os.path.exists(concepts_path):
        try:
            backend_dir = os.path.dirname(os.path.abspath(__file__))
            get_concepts_proc = subprocess.Popen([
                sys.executable, "-c", "from concepts import getConcepts; getConcepts()"
            ], cwd=os.path.dirname(backend_dir))
            getConcepts_started = True
            logging.debug("[sidecar] 启动时检测到缺少 concepts.csv，已自动调用 getConcepts 子进程")

            # 等待getConcepts完成
            logging.debug("[sidecar] 等待 getConcepts 完成...")
            return_code = get_concepts_proc.wait()
            if return_code == 0:
                logging.debug("[sidecar] getConcepts 执行成功")
                # 重新获取concepts_path，因为文件现在应该存在了
                concepts_path = get_resource_path("static/concepts.csv")
            else:
                logging.debug(f"[sidecar] getConcepts 执行失败，返回码: {return_code}")
        except Exception as e:
            logging.debug(f"[sidecar] 自动调用 getConcepts 失败: {e}")
    else:
        logging.debug("[sidecar] 检测到已存在 concepts.csv，跳过自动调用 getConcepts")

    # Load concepts data regardless of whether it was just created or already existed
    try:
        # 检查文件是否存在
        if not concepts_path or not os.path.exists(concepts_path):
            if getConcepts_started:
                logging.debug("[sidecar] getConcepts完成后仍无法找到concepts.csv文件，服务无法启动")
            else:
                logging.debug("[sidecar] concepts.csv文件不存在，服务无法启动")
            raise RuntimeError("concepts.csv文件不存在，无法启动服务")
        else:
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            # 读取concepts.csv
            concepts_df = pd.read_csv(concepts_path, dtype={'板块代码': str, '板块名称': str})
            logging.debug(f"[sidecar] concepts.csv读取完成，长度: {len(concepts_df)}")
            # 读取concept_stocks.csv
            concept_stocks_path = get_resource_path("static/concept_stocks.csv")
            if not concept_stocks_path or not os.path.exists(concept_stocks_path):
                logging.debug("[sidecar] concept_stocks.csv文件不存在，服务无法启动")
                raise RuntimeError("concept_stocks.csv文件不存在，无法启动服务")
            concept_stocks_df = pd.read_csv(concept_stocks_path, dtype={'板块代码': str, '股票代码': str})
            logging.debug(f"[sidecar] concept_stocks.csv读取完成，长度: {len(concept_stocks_df)}")
            # 合并
            concept_df = pd.merge(concept_stocks_df, concepts_df, on='板块代码', how='left')
            concept_df = concept_df[['板块代码', '板块名称', '股票代码']]
            logging.debug(f"[sidecar] 合并后concept_df长度: {len(concept_df)}")
            # 清理NaN值
            concept_df = concept_df.fillna('').infer_objects(copy=False)
            logging.debug(f"[sidecar] 清理NaN值后的concept_df长度: {len(concept_df)}")
            if concept_df.empty:
                logging.debug("[sidecar] concepts数据合并后为空，服务无法启动")
                raise RuntimeError("concepts数据合并后为空，无法启动服务")
    except Exception as e:
        logging.debug(f"[sidecar] Error loading concepts data: {e}")
        raise RuntimeError(f"无法加载concepts数据: {e}")

    # 检查当天的changes文件是否已存在，如果存在则跳过prepareChanges
    try:
        from utils import get_latest_trade_date, setup_static_directory
        static_dir = setup_static_directory()
        current_date = get_latest_trade_date()
        changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")

        if os.path.exists(changes_path):
            logging.debug(f"[sidecar] 检测到当天的changes文件已存在: {changes_path}，跳过prepareChanges")
        else:
            logging.debug(f"[sidecar] 当天的changes文件不存在: {changes_path}，开始运行 prepareChanges...")
            backend_dir = os.path.dirname(os.path.abspath(__file__))
            prepare_proc = subprocess.Popen([
                sys.executable, "-c", f"from prepare import prepareChanges; prepareChanges('{current_date}')"
            ], cwd=os.path.dirname(backend_dir))
            logging.debug(f"[sidecar] 已启动 prepareChanges 子进程，PID: {prepare_proc.pid}")

            # 等待进程完成
            return_code = prepare_proc.wait()
            if return_code == 0:
                logging.debug("[sidecar] prepareChanges 执行成功")
            else:
                logging.debug(f"[sidecar] prepareChanges 执行失败，返回码: {return_code}")
    except Exception as e:
        logging.debug(f"[sidecar] 检查changes文件或运行 prepareChanges 失败: {e}")

    # 加载picked.csv到内存和共享内存中
    load_picked_data()

    # 获取共享内存的picked数据引用
    shared_picked_data = get_shared_picked_data()

    # 启动 worker_queue 进程，实时写入队列
    # 只有在第一次初始化或进程不存在时才启动
    if get_changes_proc is None or not get_changes_proc.is_alive():
        try:
            from worker_queue import worker as changes_worker
            # 传递concept_df、shared_picked_data和当前changes数据给worker进程
            # 读取当前的changes文件数据
            current_changes_df = None
            try:
                if os.path.exists(changes_path):
                    logging.debug(f"[sidecar] 读取当前changes文件传递给worker: {changes_path}")
                    current_changes_df = pd.read_csv(changes_path)
                    current_changes_df = current_changes_df.fillna('').infer_objects(copy=False)
                    
                    # 字段校验
                    standard_columns = ['股票代码', '时间', '名称', '相关信息', '类型', '板块名称', '四舍五入取整', '上下午', '时间排序', '标识']
                    df_columns = list(current_changes_df.columns)
                    if not all(col in df_columns for col in standard_columns):
                        logging.debug(f"[backend_service] changes文件字段不匹配，收到字段: {df_columns}")
                        logging.debug(f"[backend_service] 内容预览: {current_changes_df.head(2).to_dict('records') if hasattr(current_changes_df, 'head') else str(current_changes_df)[:200]}")
                        current_changes_df = pd.DataFrame(columns=standard_columns)
                    else:
                        logging.debug(f"[sidecar] 读取到changes数据，记录数: {len(current_changes_df)}")
                        
                        # 覆盖picked相关股票的板块名称和板块编号
                        try:
                            from services.pick_service import get_shared_picked_df
                            picked_df = get_shared_picked_df()
                            
                            if picked_df is not None and not picked_df.empty:
                                logging.debug(f"[sidecar] 开始覆盖picked股票的板块信息，picked_df长度: {len(picked_df)}")
                                
                                # 确保股票代码列的数据类型一致
                                current_changes_df['股票代码'] = current_changes_df['股票代码'].astype(str)
                                picked_df['股票代码'] = picked_df['股票代码'].astype(str)
                                
                                # 创建picked股票代码到板块信息的映射
                                picked_stock_to_sector_map = dict(zip(picked_df['股票代码'], picked_df['板块名称']))
                                picked_stock_to_sector_code_map = dict(zip(picked_df['股票代码'], picked_df['板块代码']))
                                
                                # 找到在changes中的picked股票
                                picked_stocks_in_changes = current_changes_df[current_changes_df['股票代码'].isin(picked_df['股票代码'])]
                                logging.debug(f"[sidecar] 在changes中找到{len(picked_stocks_in_changes)}只picked股票")
                                
                                if not picked_stocks_in_changes.empty:
                                    # 覆盖板块名称
                                    current_changes_df.loc[current_changes_df['股票代码'].isin(picked_df['股票代码']), '板块名称'] = \
                                        current_changes_df.loc[current_changes_df['股票代码'].isin(picked_df['股票代码']), '股票代码'].map(picked_stock_to_sector_map)
                                    
                                    # 添加板块代码列（如果不存在）
                                    if '板块代码' not in current_changes_df.columns:
                                        current_changes_df['板块代码'] = ''
                                    
                                    # 覆盖板块代码
                                    current_changes_df.loc[current_changes_df['股票代码'].isin(picked_df['股票代码']), '板块代码'] = \
                                        current_changes_df.loc[current_changes_df['股票代码'].isin(picked_df['股票代码']), '股票代码'].map(picked_stock_to_sector_code_map)
                                    
                                    logging.debug(f"[sidecar] 已覆盖{len(picked_stocks_in_changes)}只picked股票的板块信息")
                                    
                                    # 记录覆盖的股票信息
                                    covered_stocks = current_changes_df[current_changes_df['股票代码'].isin(picked_df['股票代码'])][['股票代码', '名称', '板块名称', '板块代码']]
                                    logging.debug(f"[sidecar] 覆盖的股票信息: {covered_stocks.to_dict('records')}")
                                else:
                                    logging.debug(f"[sidecar] 在changes中未找到picked股票")
                            else:
                                logging.debug(f"[sidecar] picked_df为空，跳过板块信息覆盖")
                                
                        except Exception as e:
                            logging.debug(f"[sidecar] 覆盖picked股票板块信息时出错: {e}")
                            import traceback
                            logging.debug(f"[sidecar] 详细错误信息: {traceback.format_exc()}")
                else:
                    logging.debug(f"[sidecar] changes文件不存在，传递空DataFrame: {changes_path}")
            except Exception as e:
                logging.debug(f"[sidecar] 读取changes文件失败: {e}")
                current_changes_df = None

            get_changes_proc = Process(
                target=changes_worker,
                args=(
                    buffer_queue,
                    2,
                    concept_df.copy() if concept_df is not None else None,  # initial_concept_df
                    current_changes_df.copy() if current_changes_df is not None else None,  # initial_changes_df
                    300,  # batch_interval
                    shared_picked_data  # shared_picked_data
                ),
                daemon=True
            )
            get_changes_proc.start()
            logging.debug("[sidecar] 已启动 worker_queue 子进程")
        except Exception as e:
            logging.debug(f"[sidecar] 启动 worker_queue 子进程失败: {e}")
    else:
        logging.debug("[sidecar] worker_queue 子进程已在运行，跳过启动")

    initialization_completed = True
    logging.debug("[sidecar] 后端服务初始化完成")


def start_get_concepts():
    """启动获取概念数据的子进程"""
    global get_concepts_proc
    # 检查子进程是否正在运行
    if get_concepts_proc is not None and get_concepts_proc.poll() is None:
        return {"status": "already running", "pid": get_concepts_proc.pid}

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    # 启动子进程执行 getConcepts
    get_concepts_proc = subprocess.Popen([
        sys.executable, "-c", "from concepts import getConcepts; getConcepts()"
    ], cwd=backend_dir)
    return {"status": "started", "pid": get_concepts_proc.pid}


def queue_get_concepts():
    """将getConcepts任务加入队列执行一次"""
    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # 启动子进程执行 getConcepts
        proc = subprocess.Popen([
            sys.executable, "-c", "from concepts import getConcepts; getConcepts()"
        ], cwd=backend_dir)
        logging.debug(f"[queue_get_concepts] 已将getConcepts任务加入队列执行，PID: {proc.pid}")
        return {"status": "queued", "pid": proc.pid, "message": "getConcepts任务已加入队列执行"}
    except Exception as e:
        logging.debug(f"[queue_get_concepts] 执行getConcepts任务失败: {e}")
        return {"status": "error", "message": str(e)}


def reload_concept_df():
    """重新加载concept_df"""
    global concept_df
    try:
        concepts_path = get_resource_path("static/concepts.csv")
        if not concepts_path or not os.path.exists(concepts_path):
            logging.debug("[reload_concept_df] concepts.csv文件不存在")
            return {"status": "error", "message": "concepts.csv文件不存在"}

        # 重新加载concepts数据
        dtype_dict = {'股票代码': str, '板块代码': str}
        concept_df = pd.read_csv(concepts_path, dtype=dtype_dict)
        concept_df = concept_df.fillna('').infer_objects(copy=False)

        logging.debug(f"[reload_concept_df] 重新加载concept_df成功，记录数: {len(concept_df)}")

        return {"status": "success", "message": "概念数据重新加载成功"}

    except Exception as e:
        logging.debug(f"[reload_concept_df] 重新加载concept_df失败: {e}")
        return {"status": "error", "message": str(e)}



def search_concepts(query):
    """搜索concept_df中的股票，直接从文件读取以获取完整数据"""
    try:
        if not query:
            return {"status": "success", "data": []}

        # 直接读取concepts.csv文件，不使用内存中的concept_df（可能被排序处理过）
        concepts_path = get_resource_path("static/concepts.csv")
        if not concepts_path or not os.path.exists(concepts_path):
            return {"status": "error", "message": "概念数据文件不存在"}
        concepts_df = pd.read_csv(concepts_path)
        concept_stocks_df = pd.read_csv(get_resource_path("static/concept_stocks.csv"), dtype={'板块代码': str, '股票代码': str})
        logging.debug(f"[sidecar] concept_stocks.csv读取完成，长度: {len(concept_stocks_df)}")
        # 合并
        original_concept_df = pd.merge(concept_stocks_df, concepts_df, on='板块代码', how='left')

        logging.debug(f"[api/concepts/search] 读取到{len(original_concept_df)}条原始概念数据")

        # 首先尝试精确匹配股票代码
        logging.debug(f"[api/concepts/search] 查询参数: '{query}'")
        exact_match_df = original_concept_df[
            original_concept_df['股票代码'].astype(str) == query
        ]


        if not exact_match_df.empty:
            # 如果找到精确匹配的股票代码，返回该股票的所有板块记录（不去重）
            results = exact_match_df[['股票代码', '板块代码', '板块名称']]
            logging.debug(f"[api/concepts/search] 精确匹配股票代码'{query}'，找到{len(results)}条板块记录")

            # 显示所有记录
            for _, row in results.iterrows():
                logging.debug(f"  - {row['股票代码']} | {row['板块代码']} | {row['板块名称']}")
        else:
            # 如果没有精确匹配，进行模糊搜索
            logging.debug(f"[api/concepts/search] 未找到精确匹配，进行模糊搜索")
            filtered_df = original_concept_df[
                original_concept_df['股票代码'].astype(str).str.contains(query, na=False)
            ]

            # 模糊搜索时也不去重，让前端自己处理
            results = filtered_df[['股票代码', '板块代码', '板块名称']].head(50)
            logging.debug(f"[api/concepts/search] 模糊搜索'{query}'，找到{len(results)}条结果")

        # 清理NaN值
        results = results.fillna('')
        try:
            if len(query)==6:
                symbol = 'SZ'+query
                if query.startswith('6'):
                    symbol='SH'+query
                if query.startswith('8'):
                    symbol='BJ'+query
                stock_individual_basic_info_xq_df = ak.stock_individual_basic_info_xq(symbol=symbol)
                logging.debug(f"[search_concepts] 获取股票基本信息，DataFrame长度: {len(stock_individual_basic_info_xq_df)}")

                # 获取item列为'org_short_name_cn'的行
                target_row = stock_individual_basic_info_xq_df[stock_individual_basic_info_xq_df['item'] == 'org_short_name_cn']
                if not target_row.empty:
                    stock_name = target_row['value'].values[0]
                    logging.debug(f"[search_concepts] 获取到股票名称: {stock_name}")
                    results['股票名称'] = stock_name
                else:
                    logging.debug(f"[search_concepts] 未找到org_short_name_cn对应的股票名称")
        except Exception as e:
            logging.debug(f"[search_concepts] 获取股票名称失败: {e}")

        # 转换为字典并清理NaN值
        records = results.to_dict('records')
        records = clean_nan_values(records)

        return {"status": "success", "data": records}

    except Exception as e:
        logging.debug(f"[api/concepts/search] 搜索失败: {e}")
        return {"status": "error", "message": str(e)}




def get_concept_sectors():
    """获取concept_df中的所有板块"""
    global concept_df
    try:
        if concept_df is None or concept_df.empty:
            return {"status": "error", "message": "概念数据未加载"}

        # 获取所有板块信息并去重
        sectors = concept_df[['板块代码', '板块名称']].drop_duplicates()

        # 清理NaN值
        sectors = sectors.fillna('').infer_objects(copy=False)

        # 转换为字典并清理NaN值
        records = sectors.to_dict('records')
        records = clean_nan_values(records)

        logging.debug(f"[api/concepts/sectors] 获取板块列表，共{len(records)}个板块")
        return {"status": "success", "data": records}

    except Exception as e:
        logging.debug(f"[api/concepts/sectors] 获取板块列表失败: {e}")
        return {"status": "error", "message": str(e)}


def get_stock_sectors(stock_code):
    """获取指定股票在concept_df中对应的所有板块"""
    global concept_df
    try:
        if concept_df is None or concept_df.empty:
            return {"status": "error", "message": "概念数据未加载"}

        # 查找指定股票的所有板块
        stock_sectors = concept_df[concept_df['股票代码'] == stock_code][['板块代码', '板块名称']].drop_duplicates()

        # 清理NaN值
        stock_sectors = stock_sectors.fillna('').infer_objects(copy=False)

        # 转换为字典并清理NaN值
        records = stock_sectors.to_dict('records')
        records = clean_nan_values(records)

        logging.debug(f"[api/concepts/stock-sectors] 获取股票{stock_code}的板块列表，共{len(records)}个板块")
        return {"status": "success", "data": records}

    except Exception as e:
        logging.debug(f"[api/concepts/stock-sectors] 获取股票板块列表失败: {e}")
        return {"status": "error", "message": str(e)}


def get_watch_status(watch_process):
    """Get the status of the fluctuation watch process"""
    if watch_process is None:
        return {"status": "not_running"}

    return_code = watch_process.poll()
    if return_code is None:
        return {"status": "running", "pid": watch_process.pid}
    else:
        return {"status": "stopped", "return_code": return_code}
