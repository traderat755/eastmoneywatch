import os
import sys
import subprocess
import pandas as pd
from multiprocessing import Process
from utils import get_resource_path, get_data_dir


# Global variables
concept_df = None
get_concepts_proc = None
get_changes_proc = None
initialization_completed = False


def initialize_backend_services(buffer_queue):
    """初始化后端服务，启动必要的子进程"""
    global concept_df, get_concepts_proc, get_changes_proc, initialization_completed

    print("[sidecar] 开始初始化后端服务...", flush=True)

    # Check if concepts.csv exists before calling getConcepts
    concepts_path = get_resource_path("static/concepts.csv")
    if not concepts_path or not os.path.exists(concepts_path):
        try:
            backend_dir = os.path.dirname(os.path.abspath(__file__))
            get_concepts_proc = subprocess.Popen([
                sys.executable, "-c", "from concepts import getConcepts; getConcepts()"
            ], cwd=os.path.dirname(backend_dir))
            print("[sidecar] 启动时检测到缺少 concepts.csv，已自动调用 getConcepts 子进程", flush=True)
        except Exception as e:
            print(f"[sidecar] 自动调用 getConcepts 失败: {e}", flush=True)
    else:
        print("[sidecar] 检测到已存在 concepts.csv，跳过自动调用 getConcepts", flush=True)

    # Load concepts data regardless of whether it was just created or already existed
    try:
        # 确保股票代码列被读取为字符串类型
        dtype_dict = {'股票代码': str, '板块代码': str}
        concept_df = pd.read_csv(concepts_path, dtype=dtype_dict)
        print("[sidecar] Successfully loaded concepts data from: " + concepts_path, flush=True)
        print(f"[sidecar] 原始concept_df长度: {len(concept_df)}", flush=True)
        print(f"[sidecar] 股票代码列数据类型: {concept_df['股票代码'].dtype}", flush=True)
    except Exception as e:
        print(f"[sidecar] Error loading concepts data: {e}", flush=True)
        concept_df = pd.DataFrame()  # Create empty DataFrame if loading fails

    # 运行 prepareChanges 获取大笔买入数据
    try:
        print("[sidecar] 开始运行 prepareChanges...", flush=True)
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        prepare_proc = subprocess.Popen([
            sys.executable, "-c", "from prepare import prepareChanges; prepareChanges()"
        ], cwd=os.path.dirname(backend_dir))
        print(f"[sidecar] 已启动 prepareChanges 子进程，PID: {prepare_proc.pid}", flush=True)
        
        # 等待进程完成
        return_code = prepare_proc.wait()
        if return_code == 0:
            print("[sidecar] prepareChanges 执行成功", flush=True)
        else:
            print(f"[sidecar] prepareChanges 执行失败，返回码: {return_code}", flush=True)
    except Exception as e:
        print(f"[sidecar] 运行 prepareChanges 失败: {e}", flush=True)

    # 加载picked.csv并重新排序concept_df
    try:
        picked_path = get_resource_path("static/picked.csv")
        if picked_path and os.path.exists(picked_path):
            print(f"[sidecar] 发现picked.csv文件: {picked_path}", flush=True)
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)
            print(f"[sidecar] 加载picked.csv成功，共{len(picked_df)}条记录", flush=True)
            print(f"[sidecar] picked.csv字段: {list(picked_df.columns)}", flush=True)
            print(f"[sidecar] picked.csv股票代码列数据类型: {picked_df['股票代码'].dtype}", flush=True)

            # 将选中的板块排到最前面
            if not concept_df.empty and not picked_df.empty:
                # 获取选中的板块代码列表（去重）
                picked_sector_codes = picked_df['板块代码'].unique().tolist()
                print(f"[sidecar] 选中的板块代码: {picked_sector_codes}", flush=True)

                # 检查concept_df中是否有板块代码字段
                if '板块代码' in concept_df.columns:
                    # 从concept_df中去掉picked的股票
                    picked_stock_codes = picked_df['股票代码'].unique().tolist()
                    print(f"[sidecar] 需要从concept_df中移除的股票代码: {picked_stock_codes}", flush=True)

                    # 记录移除前的concept_df长度
                    original_concept_len = len(concept_df)
                    print(f"[sidecar] 移除前concept_df长度: {original_concept_len}", flush=True)

                    # 移除picked中的股票
                    concept_df = concept_df[~concept_df['股票代码'].isin(picked_stock_codes)]
                    removed_count = original_concept_len - len(concept_df)
                    print(f"[sidecar] 从concept_df中移除了{removed_count}条记录", flush=True)

                    # 将picked的股票拼接到concept_df前面
                    concept_df = pd.concat([picked_df, concept_df], ignore_index=True)
                    print(f"[sidecar] 将picked_df拼接到concept_df前面，最终长度: {len(concept_df)}", flush=True)

                    # 创建排序索引：选中的板块在前，其他板块在后
                    concept_df['_sort_order'] = concept_df['板块代码'].apply(
                        lambda x: picked_sector_codes.index(x) if x in picked_sector_codes else len(picked_sector_codes)
                    )

                    # 按排序索引排序
                    concept_df = concept_df.sort_values('_sort_order').drop('_sort_order', axis=1)
                    print(f"[sidecar] 已重新排序concept_df，选中的板块排在最前面", flush=True)

                    # 打印排序后的前几个板块
                    first_sectors = concept_df['板块代码'].head(10).unique()
                    print(f"[sidecar] 排序后前10个板块: {first_sectors.tolist()}", flush=True)
                else:
                    print(f"[sidecar] concept_df中没有板块代码字段，无法重新排序", flush=True)
                    print(f"[sidecar] concept_df字段: {list(concept_df.columns)}", flush=True)
        else:
            print("[sidecar] 未发现picked.csv文件，使用原始concept_df", flush=True)
    except Exception as e:
        print(f"[sidecar] 处理picked.csv时出错: {e}", flush=True)

    # 启动 get_changes_worker_queue 进程，实时写入队列
    # 只有在第一次初始化或进程不存在时才启动
    if get_changes_proc is None or not get_changes_proc.is_alive():
        try:
            from get_changes_worker_queue import worker as changes_worker
            get_changes_proc = Process(target=changes_worker, args=(concept_df, buffer_queue, 2), daemon=True)
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


def get_picked_stocks():
    """获取选中的股票列表"""
    try:
        picked_path = get_resource_path("static/picked.csv")
        if picked_path and os.path.exists(picked_path):
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)
            print(f"[api/picked] 获取选中股票列表，共{len(picked_df)}条记录", flush=True)
            return {"status": "success", "data": picked_df.to_dict('records')}
        else:
            print("[api/picked] picked.csv文件不存在", flush=True)
            return {"status": "success", "data": []}
    except Exception as e:
        print(f"[api/picked] 获取选中股票列表失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def add_picked_stock(stock_data):
    """添加股票到精选列表"""
    try:
        picked_path = get_resource_path("static/picked.csv")
        
        # 转换为字典
        stock_dict = stock_data.dict()
        
        # 读取现有数据
        if picked_path and os.path.exists(picked_path):
            # 确保股票代码列被读取为字符串类型
            dtype_dict = {'股票代码': str, '板块代码': str}
            picked_df = pd.read_csv(picked_path, dtype=dtype_dict)
        else:
            picked_df = pd.DataFrame(columns=['股票代码', '股票名称', '板块代码', '板块名称'])
        
        # 检查是否已存在
        if not picked_df.empty and stock_dict['股票代码'] in picked_df['股票代码'].values:
            return {"status": "error", "message": "股票已存在于精选列表中"}
        
        # 添加新股票
        new_stock = pd.DataFrame([stock_dict])
        picked_df = pd.concat([picked_df, new_stock], ignore_index=True)
        
        # 保存到文件
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
    try:
        picked_path = get_resource_path("static/picked.csv")
        if not picked_path or not os.path.exists(picked_path):
            return {"status": "error", "message": "精选列表文件不存在"}
        
        # 确保股票代码列被读取为字符串类型
        dtype_dict = {'股票代码': str, '板块代码': str}
        picked_df = pd.read_csv(picked_path, dtype=dtype_dict)
        
        # 查找股票
        stock_index = picked_df[picked_df['股票代码'] == stock_code].index
        if len(stock_index) == 0:
            return {"status": "error", "message": "股票不存在于精选列表中"}
        
        # 更新股票信息
        stock_dict = stock_data.dict()
        for key, value in stock_dict.items():
            if key in picked_df.columns:
                picked_df.loc[stock_index[0], key] = value
        
        # 保存到文件
        picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        
        print(f"[api/picked] 更新股票成功: {stock_code}", flush=True)
        return {"status": "success", "message": "股票更新成功"}
    
    except Exception as e:
        print(f"[api/picked] 更新股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def delete_picked_stock(stock_code):
    """从精选列表中删除股票"""
    try:
        picked_path = get_resource_path("static/picked.csv")
        if not picked_path or not os.path.exists(picked_path):
            return {"status": "error", "message": "精选列表文件不存在"}
        
        # 确保股票代码列被读取为字符串类型
        dtype_dict = {'股票代码': str, '板块代码': str}
        picked_df = pd.read_csv(picked_path, dtype=dtype_dict)
        
        # 检查股票是否存在
        if stock_code not in picked_df['股票代码'].values:
            return {"status": "error", "message": "股票不存在于精选列表中"}
        
        # 删除股票
        picked_df = picked_df[picked_df['股票代码'] != stock_code]
        
        # 保存到文件
        picked_df.to_csv(picked_path, index=False, encoding='utf-8')
        
        print(f"[api/picked] 删除股票成功: {stock_code}", flush=True)
        return {"status": "success", "message": "股票删除成功"}
    
    except Exception as e:
        print(f"[api/picked] 删除股票失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


def search_concepts(query):
    """搜索concept_df中的股票"""
    global concept_df
    try:
        if concept_df is None or concept_df.empty:
            return {"status": "error", "message": "概念数据未加载"}
        
        if not query:
            return {"status": "success", "data": []}
        
        # 搜索股票名称或代码
        filtered_df = concept_df[
            concept_df['股票名称'].astype(str).str.contains(query, na=False) | 
            concept_df['股票代码'].astype(str).str.contains(query, na=False)
        ]
        
        # 去重并限制结果数量
        results = filtered_df[['股票代码', '股票名称', '板块代码', '板块名称']].drop_duplicates().head(50)
        
        print(f"[api/concepts/search] 搜索'{query}'，找到{len(results)}条结果", flush=True)
        return {"status": "success", "data": results.to_dict('records')}
    
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
        
        print(f"[api/concepts/sectors] 获取板块列表，共{len(sectors)}个板块", flush=True)
        return {"status": "success", "data": sectors.to_dict('records')}
    
    except Exception as e:
        print(f"[api/concepts/sectors] 获取板块列表失败: {e}", flush=True)
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