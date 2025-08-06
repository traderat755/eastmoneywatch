import os
import sys
import subprocess
import pandas as pd
from multiprocessing import Process
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


# Global variables
concept_df = None
picked_df = None  # 内存中的picked数据，避免频繁读写文件
get_concepts_proc = None
get_changes_proc = None
initialization_completed = False


def initialize_backend_services(buffer_queue):
    """初始化后端服务，启动必要的子进程"""
    global concept_df, picked_df, get_concepts_proc, get_changes_proc, initialization_completed

    print("[sidecar] 开始初始化后端服务...", flush=True)

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
            print("[sidecar] 启动时检测到缺少 concepts.csv，已自动调用 getConcepts 子进程", flush=True)
            
            # 等待getConcepts完成
            print("[sidecar] 等待 getConcepts 完成...", flush=True)
            return_code = get_concepts_proc.wait()
            if return_code == 0:
                print("[sidecar] getConcepts 执行成功", flush=True)
                # 重新获取concepts_path，因为文件现在应该存在了
                concepts_path = get_resource_path("static/concepts.csv")
            else:
                print(f"[sidecar] getConcepts 执行失败，返回码: {return_code}", flush=True)
        except Exception as e:
            print(f"[sidecar] 自动调用 getConcepts 失败: {e}", flush=True)
    else:
        print("[sidecar] 检测到已存在 concepts.csv，跳过自动调用 getConcepts", flush=True)

    # Load concepts data regardless of whether it was just created or already existed
    try:
        # 检查文件是否存在
        if not concepts_path or not os.path.exists(concepts_path):
            if getConcepts_started:
                print("[sidecar] getConcepts完成后仍无法找到concepts.csv文件，服务无法启动", flush=True)
            else:
                print("[sidecar] concepts.csv文件不存在，服务无法启动", flush=True)
            raise RuntimeError("concepts.csv文件不存在，无法启动服务")
        else:
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            concept_df = pd.read_csv(concepts_path, dtype=dtype_dict)

            # 清理NaN值
            concept_df = concept_df.fillna('')

            if concept_df.empty:
                print("[sidecar] concepts.csv文件为空，服务无法启动", flush=True)
                raise RuntimeError("concepts.csv文件为空，无法启动服务")

            print("[sidecar] Successfully loaded concepts data from: " + concepts_path, flush=True)
            print(f"[sidecar] 原始concept_df长度: {len(concept_df)}", flush=True)
            print(f"[sidecar] 股票代码列数据类型: {concept_df['股票代码'].dtype}", flush=True)
            print(f"[sidecar] 清理NaN值后的concept_df长度: {len(concept_df)}", flush=True)
    except Exception as e:
        print(f"[sidecar] Error loading concepts data: {e}", flush=True)
        raise RuntimeError(f"无法加载concepts数据: {e}")

    # 检查当天的changes文件是否已存在，如果存在则跳过prepareChanges
    try:
        from utils import get_latest_trade_date, setup_static_directory
        static_dir = setup_static_directory()
        current_date = get_latest_trade_date()
        changes_path = os.path.join(static_dir, f"changes_{current_date}.csv")
        
        if os.path.exists(changes_path):
            print(f"[sidecar] 检测到当天的changes文件已存在: {changes_path}，跳过prepareChanges", flush=True)
        else:
            print(f"[sidecar] 当天的changes文件不存在: {changes_path}，开始运行 prepareChanges...", flush=True)
            backend_dir = os.path.dirname(os.path.abspath(__file__))
            prepare_proc = subprocess.Popen([
                sys.executable, "-c", f"from prepare import prepareChanges; prepareChanges('{current_date}')"
            ], cwd=os.path.dirname(backend_dir))
            print(f"[sidecar] 已启动 prepareChanges 子进程，PID: {prepare_proc.pid}", flush=True)

            # 等待进程完成
            return_code = prepare_proc.wait()
            if return_code == 0:
                print("[sidecar] prepareChanges 执行成功", flush=True)
            else:
                print(f"[sidecar] prepareChanges 执行失败，返回码: {return_code}", flush=True)
    except Exception as e:
        print(f"[sidecar] 检查changes文件或运行 prepareChanges 失败: {e}", flush=True)

    # 加载picked.csv到内存中并重新排序concept_df
    try:
        picked_path = get_resource_path("static/picked.csv")
        if picked_path and os.path.exists(picked_path):
            print(f"[sidecar] 发现picked.csv文件: {picked_path}", flush=True)
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)

            # 清理NaN值
            picked_df = picked_df.fillna('')

            print(f"[sidecar] 加载picked.csv到内存成功，共{len(picked_df)}条记录", flush=True)
            print(f"[sidecar] picked.csv字段: {list(picked_df.columns)}", flush=True)
            print(f"[sidecar] picked.csv股票代码列数据类型: {picked_df['股票代码'].dtype}", flush=True)
        else:
            print("[sidecar] 未发现picked.csv文件，创建空的picked_df", flush=True)
            picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

        # 更新worker进程中的concept_df和picked_df缓存
        from get_changes_worker_queue import update_concept_df_cache, update_picked_df_cache
        update_concept_df_cache(concept_df.copy())
        update_picked_df_cache(picked_df.copy())

    except Exception as e:
        print(f"[sidecar] 处理picked.csv时出错: {e}", flush=True)
        picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])

    # 启动 get_changes_worker_queue 进程，实时写入队列
    # 只有在第一次初始化或进程不存在时才启动
    if get_changes_proc is None or not get_changes_proc.is_alive():
        try:
            from get_changes_worker_queue import worker as changes_worker
            # 传递concept_df和picked_df给worker进程
            get_changes_proc = Process(target=changes_worker, args=(buffer_queue, 2, concept_df.copy() if concept_df is not None else None, picked_df.copy() if picked_df is not None else None), daemon=True)
            get_changes_proc.start()
            print("[sidecar] 已启动 get_changes_worker_queue 子进程", flush=True)
        except Exception as e:
            print(f"[sidecar] 启动 get_changes_worker_queue 子进程失败: {e}", flush=True)
    else:
        print("[sidecar] get_changes_worker_queue 子进程已在运行，跳过启动", flush=True)

    initialization_completed = True
    print("[sidecar] 后端服务初始化完成", flush=True)


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
        print(f"[queue_get_concepts] 已将getConcepts任务加入队列执行，PID: {proc.pid}", flush=True)
        return {"status": "queued", "pid": proc.pid, "message": "getConcepts任务已加入队列执行"}
    except Exception as e:
        print(f"[queue_get_concepts] 执行getConcepts任务失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def reload_concept_df():
    """重新加载concept_df并更新worker缓存"""
    global concept_df, picked_df
    try:
        concepts_path = get_resource_path("static/concepts.csv")
        if not concepts_path or not os.path.exists(concepts_path):
            print("[reload_concept_df] concepts.csv文件不存在", flush=True)
            return {"status": "error", "message": "concepts.csv文件不存在"}
        
        # 重新加载concepts数据
        dtype_dict = {'股票代码': str, '板块代码': str}
        concept_df = pd.read_csv(concepts_path, dtype=dtype_dict)
        concept_df = concept_df.fillna('')
        
        print(f"[reload_concept_df] 重新加载concept_df成功，记录数: {len(concept_df)}", flush=True)
        
        # 更新worker进程中的concept_df缓存
        from get_changes_worker_queue import update_concept_df_cache
        update_concept_df_cache(concept_df.copy())
        print("[reload_concept_df] 已更新worker进程中的concept_df缓存", flush=True)
        
        return {"status": "success", "message": "概念数据重新加载成功"}
        
    except Exception as e:
        print(f"[reload_concept_df] 重新加载concept_df失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


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

        # 更新worker进程中的picked_df缓存
        from get_changes_worker_queue import update_picked_df_cache
        update_picked_df_cache(picked_df.copy())

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

        # 更新worker进程中的picked_df缓存
        from get_changes_worker_queue import update_picked_df_cache
        update_picked_df_cache(picked_df.copy())

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

        # 更新worker进程中的picked_df缓存
        from get_changes_worker_queue import update_picked_df_cache
        update_picked_df_cache(picked_df.copy())

        print(f"[api/picked] 删除股票成功: {stock_code}", flush=True)
        return {"status": "success", "message": "股票删除成功"}

    except Exception as e:
        print(f"[api/picked] 删除股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def get_current_picked_df():
    """获取当前内存中的picked_df，供worker进程使用"""
    global picked_df
    return picked_df


def search_concepts(query):
    """搜索concept_df中的股票，直接从文件读取以获取完整数据"""
    try:
        if not query:
            return {"status": "success", "data": []}

        # 直接读取concepts.csv文件，不使用内存中的concept_df（可能被排序处理过）
        concepts_path = get_resource_path("static/concepts.csv")
        if not concepts_path or not os.path.exists(concepts_path):
            return {"status": "error", "message": "概念数据文件不存在"}

        print(f"[api/concepts/search] 直接从文件读取concepts数据进行搜索: {concepts_path}", flush=True)
        
        # 读取完整的原始数据
        dtype_dict = {'股票代码': str, '板块代码': str}
        original_concept_df = pd.read_csv(concepts_path, dtype=dtype_dict)
        original_concept_df = original_concept_df.fillna('')
        
        print(f"[api/concepts/search] 读取到{len(original_concept_df)}条原始概念数据", flush=True)

        # 首先尝试精确匹配股票代码
        print(f"[api/concepts/search] 查询参数: '{query}'", flush=True)
        exact_match_df = original_concept_df[
            original_concept_df['股票代码'].astype(str) == query
        ]
        
        if not exact_match_df.empty:
            # 如果找到精确匹配的股票代码，返回该股票的所有板块记录（不去重）
            results = exact_match_df[['股票代码', '股票名称', '板块代码', '板块名称']]
            print(f"[api/concepts/search] 精确匹配股票代码'{query}'，找到{len(results)}条板块记录", flush=True)
            
            # 显示所有记录
            for _, row in results.iterrows():
                print(f"  - {row['股票代码']} | {row['股票名称']} | {row['板块代码']} | {row['板块名称']}", flush=True)
        else:
            # 如果没有精确匹配，进行模糊搜索
            print(f"[api/concepts/search] 未找到精确匹配，进行模糊搜索", flush=True)
            filtered_df = original_concept_df[
                original_concept_df['股票名称'].astype(str).str.contains(query, na=False) |
                original_concept_df['股票代码'].astype(str).str.contains(query, na=False)
            ]
            
            # 模糊搜索时也不去重，让前端自己处理
            results = filtered_df[['股票代码', '股票名称', '板块代码', '板块名称']].head(50)
            print(f"[api/concepts/search] 模糊搜索'{query}'，找到{len(results)}条结果", flush=True)

        # 清理NaN值
        results = results.fillna('')

        # 转换为字典并清理NaN值
        records = results.to_dict('records')
        records = clean_nan_values(records)

        return {"status": "success", "data": records}

    except Exception as e:
        print(f"[api/concepts/search] 搜索失败: {e}", flush=True)
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
        sectors = sectors.fillna('')

        # 转换为字典并清理NaN值
        records = sectors.to_dict('records')
        records = clean_nan_values(records)

        print(f"[api/concepts/sectors] 获取板块列表，共{len(records)}个板块", flush=True)
        return {"status": "success", "data": records}

    except Exception as e:
        print(f"[api/concepts/sectors] 获取板块列表失败: {e}", flush=True)
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
        stock_sectors = stock_sectors.fillna('')

        # 转换为字典并清理NaN值
        records = stock_sectors.to_dict('records')
        records = clean_nan_values(records)

        print(f"[api/concepts/stock-sectors] 获取股票{stock_code}的板块列表，共{len(records)}个板块", flush=True)
        return {"status": "success", "data": records}

    except Exception as e:
        print(f"[api/concepts/stock-sectors] 获取股票板块列表失败: {e}", flush=True)
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
