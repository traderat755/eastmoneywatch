import os
import sys
import asyncio
import threading
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server
import subprocess
import pandas as pd
from collections import deque
from datetime import datetime
from multiprocessing import Process, Queue
from utils import get_resource_path, setup_static_directory, get_data_dir

# 主进程与子进程共享的队列
buffer_queue = Queue(maxsize=200)


# Global variables
server_instance = None  # Global reference to the Uvicorn server instance
concept_df = None  # Global variable for concepts data
log_messages = deque(maxlen=1000)  # Store last 1000 log messages
active_websockets = set()  # Store active WebSocket connections
watch_process = None  # Global variable for watch process
initialization_completed = False  # 标记是否已完成初始化


app = FastAPI(
    title="API server",
    version="0.1.0",
)

# getConcepts 子进程管理
get_concepts_proc = None
# getChanges 子进程管理
get_changes_proc = None
get_changes_timer = None
get_changes_stop_event = threading.Event()

def initialize_backend_services():
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
            ], cwd=backend_dir)
            print("[sidecar] 启动时检测到缺少 concepts.csv，已自动调用 getConcepts 子进程", flush=True)
        except Exception as e:
            print(f"[sidecar] 自动调用 getConcepts 失败: {e}", flush=True)
    else:
        print("[sidecar] 检测到已存在 concepts.csv，跳过自动调用 getConcepts", flush=True)

    # Load concepts data regardless of whether it was just created or already existed
    try:
        concept_df = pd.read_csv(concepts_path)
        print("[sidecar] Successfully loaded concepts data from: " + concepts_path, flush=True)
        print(f"[sidecar] 原始concept_df长度: {len(concept_df)}", flush=True)
    except Exception as e:
        print(f"[sidecar] Error loading concepts data: {e}", flush=True)
        concept_df = pd.DataFrame()  # Create empty DataFrame if loading fails

    # 运行 prepareChanges 获取大笔买入数据
    try:
        print("[sidecar] 开始运行 prepareChanges...", flush=True)
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        prepare_proc = subprocess.Popen([
            sys.executable, "-c", "from prepare import prepareChanges; prepareChanges()"
        ], cwd=backend_dir)
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
            picked_df = pd.read_csv(picked_path)
            print(f"[sidecar] 加载picked.csv成功，共{len(picked_df)}条记录", flush=True)
            print(f"[sidecar] picked.csv字段: {list(picked_df.columns)}", flush=True)

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

@app.on_event("startup")
async def startup_event():
    """FastAPI 启动时的事件处理"""
    print("[sidecar] FastAPI 应用启动中...", flush=True)
    # 在后台线程中初始化服务，避免阻塞启动
    init_thread = threading.Thread(target=initialize_backend_services)
    init_thread.daemon = True
    init_thread.start()

@app.post("/api/start_get_concepts")
def start_get_concepts():
    global get_concepts_proc
    # 检查子进程是否正在运行
    if get_concepts_proc is not None and get_concepts_proc.poll() is None:
        return {"status": "already running", "pid": get_concepts_proc.pid}
    backend_dir = os.path.dirname(os.path.abspath(__file__))
    # 启动子进程执行 getConcepts
    get_concepts_proc = subprocess.Popen([
        sys.executable, "-c", "from concepts import getConcepts; getConcepts()"
    ], cwd=backend_dir)
    return {"status": "started", "pid": get_concepts_proc.pid}


@app.get("/api/picked")
def get_picked_stocks():
    """获取选中的股票列表"""
    try:
        picked_path = get_resource_path("static/picked.csv")
        if picked_path and os.path.exists(picked_path):
            picked_df = pd.read_csv(picked_path)
            print(f"[api/picked] 获取选中股票列表，共{len(picked_df)}条记录", flush=True)
            return {"status": "success", "data": picked_df.to_dict('records')}
        else:
            print("[api/picked] picked.csv文件不存在", flush=True)
            return {"status": "success", "data": []}
    except Exception as e:
        print(f"[api/picked] 获取选中股票列表失败: {e}", flush=True)
        return {"status": "error", "message": str(e)}


# 从环境变量获取CORS origins配置
def get_cors_origins():
    """从环境变量获取CORS origins配置"""
    # 默认的端口列表
    default_ports = ["1420", "5173", "61125"]

    # 从环境变量获取CORS_PORTS
    cors_ports_env = os.getenv("CORS_PORTS")
    if cors_ports_env:
        print(f"[CORS] 从环境变量读取CORS ports: {cors_ports_env}")
        # 支持逗号分隔的多个端口
        ports = [port.strip() for port in cors_ports_env.split(",") if port.strip()]
    else:
        print(f"[CORS] 使用默认CORS ports配置")
        ports = default_ports

    # 为每个端口生成对应的origins
    origins = []
    for port in ports:
        origins.extend([
            f"http://localhost:{port}",
            f"http://127.0.0.1:{port}",
            f"http://0.0.0.0:{port}"
        ])

    # 添加特殊的tauri协议
    origins.append("tauri://localhost")

    print(f"[CORS] 生成的origins: {origins}")
    return origins

# 获取CORS origins配置
origins = get_cors_origins()
print(f"[CORS] 配置的origins: {origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,  # Required for WebSocket
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.websocket("/ws/changes")
async def websocket_changes(websocket: WebSocket):
    print(f"[ws/changes] New WebSocket connection from {websocket.client}")
    await websocket.accept()
    import json
    try:
        while True:
            # 优先从缓冲队列读取
            sent = False
            try:
                while not buffer_queue.empty():
                    data = buffer_queue.get_nowait()
                    await websocket.send_text(json.dumps(data, ensure_ascii=False))
                    sent = True
            except Exception as e:
                await websocket.send_text(json.dumps({"error": str(e)}, ensure_ascii=False))
            await asyncio.sleep(2)

    except Exception as e:
        print(f"[ws/changes] WebSocket error: {e}")
    finally:
        print(f"[ws/changes] WebSocket connection closed: {websocket.client}")
        try:
            await websocket.close()
        except RuntimeError as e:
            print(f"[ws/changes] WebSocket already closed: {e}")



@app.get("/api/watch/status")
async def get_watch_status():
    """Get the status of the fluctuation watch process"""
    global watch_process
    if watch_process is None:
        return {"status": "not_running"}

    return_code = watch_process.poll()
    if return_code is None:
        return {"status": "running", "pid": watch_process.pid}
    else:
        return {"status": "stopped", "return_code": return_code}


# This must be the last route to catch all other routes
@app.get("/{rest_of_path:path}")
async def serve_frontend(rest_of_path: str):
    dist_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dist"))
    # This is the file path to check
    file_path = os.path.join(dist_dir, rest_of_path)

    # If the file exists and it's a file, serve it
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)

    # Otherwise, it's a route for the SPA, so serve index.html
    index_path = os.path.join(dist_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)

    # If index.html doesn't exist, then something is wrong with the frontend build
    raise HTTPException(status_code=404, detail="Frontend not found. Make sure you have built the frontend and the 'dist' directory is correct.")



async def broadcast_message(message: str):
    """向所有连接的WebSocket客户端广播消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    log_messages.append(formatted_message)

    # 广播到所有活动的WebSocket连接
    disconnected = set()
    for websocket in active_websockets:
        try:
            await websocket.send_text(formatted_message)
        except Exception:
            disconnected.add(websocket)

    # 移除断开的连接
    active_websockets.difference_update(disconnected)

def output_reader(pipe, name):
    """从管道读取输出并打印"""
    for line in pipe:
        message = f"[{name}] {line.strip()}"
        print(message)
        # 使用asyncio创建一个新的事件循环来发送WebSocket消息
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(broadcast_message(message))
        loop.close()


# Programmatically force shutdown this sidecar.
def kill_process():
    global server_instance
    if server_instance is not None:
        try:
            server_instance.should_exit = True  # 通知 Uvicorn 退出
        except Exception:
            pass
    os._exit(0)  # 强制退出整个进程


# Programmatically startup the api server
def start_api_server(**kwargs):
    global server_instance
    print(f"[sidecar] 准备启动API服务器", flush=True)
    try:
        if server_instance is None:
            print(f"[sidecar] 正在启动API服务器...", flush=True)
            config = Config(app, host="0.0.0.0", log_level="info")
            server_instance = Server(config)
            # Start the ASGI server
            # Use a more robust approach to run the server
            try:
                print(f"[sidecar] 服务器配置完成，开始运行", flush=True)
                server_instance.run()
            except Exception as e:
                print(f"[sidecar] API服务器运行错误: {e}", flush=True)
        else:
            print(
                "[sidecar] 无法启动新服务器。服务器实例已在运行中。",
                flush=True,
            )
    except Exception as e:
        print(f"[sidecar] 错误，启动API服务器失败: {e}", flush=True)


# Handle the stdin event loop. This can be used like a CLI.
def stdin_loop():
    print("[sidecar] Waiting for commands...", flush=True)
    try:
        while True:
            # Read input from stdin.
            user_input = sys.stdin.readline().strip()

            # Check if the input matches one of the available functions
            match user_input:
                case "sidecar shutdown":
                    print("[sidecar] Received 'sidecar shutdown' command.", flush=True)
                    kill_process()
                case _:
                    print(
                        f"[sidecar] Invalid command [{user_input}]. Try again.", flush=True
                    )
    except Exception as e:
        print(f"[sidecar] stdin_loop error: {e}", flush=True)


# Start the input loop in a separate thread
def start_input_thread():
    try:
        input_thread = threading.Thread(target=stdin_loop)
        input_thread.daemon = True  # so it exits when the main program exits
        input_thread.start()
    except Exception as e:
        print(f"[sidecar] Failed to start input handler: {e}", flush=True)


if __name__ == "__main__":

    # 初始化后端服务
    initialize_backend_services()

    # Listen for stdin from parent process
    start_input_thread()

    # Starts API server, blocks further code from execution.
    try:
        start_api_server()
    except Exception as e:
        print(f"[sidecar] Fatal error in main thread: {e}", flush=True)
        sys.exit(1)
