import os
import sys
import asyncio
import threading
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from uvicorn import Config, Server
from collections import deque
from datetime import datetime
from multiprocessing import Queue

from config import get_cors_origins
from services.backend_service import initialize_backend_services
from routes.api import router as api_router
from routes.websocket import router as websocket_router, set_buffer_queue

# 主进程与子进程共享的队列
buffer_queue = Queue(maxsize=200)

# Global variables
server_instance = None  # Global reference to the Uvicorn server instance
log_messages = deque(maxlen=1000)  # Store last 1000 log messages
active_websockets = set()  # Store active WebSocket connections
watch_process = None  # Global variable for watch process


app = FastAPI(
    title="API server",
    version="0.1.0",
)


@app.on_event("startup")
async def startup_event():
    """FastAPI 启动时的事件处理"""
    print("[sidecar] FastAPI 应用启动中...", flush=True)
    
    # Set buffer queue for websocket module
    set_buffer_queue(buffer_queue)
    
    # 在后台线程中初始化服务，避免阻塞启动
    init_thread = threading.Thread(target=initialize_backend_services, args=(buffer_queue,))
    init_thread.daemon = True
    init_thread.start()


# Include routers
app.include_router(api_router)
app.include_router(websocket_router)

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
    # Listen for stdin from parent process
    start_input_thread()

    # Starts API server, blocks further code from execution.
    try:
        start_api_server()
    except Exception as e:
        print(f"[sidecar] Fatal error in main thread: {e}", flush=True)
        sys.exit(1)