import os
import sys
import asyncio
import threading
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from uvicorn import Config, Server
from collections import deque
from datetime import datetime
from multiprocessing import Queue
import logging

# 设置整体logging层级为debug
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

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


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """处理请求验证错误，记录详细信息"""
    logging.error(f"[validation_error] 请求验证失败: {request.url}")
    logging.error(f"[validation_error] 请求方法: {request.method}")
    logging.error(f"[validation_error] 请求头: {dict(request.headers)}")
    logging.error(f"[validation_error] 错误详情: {exc.errors()}")
    
    # 尝试读取请求体
    try:
        body = await request.body()
        logging.error(f"[validation_error] 请求体: {body.decode('utf-8')}")
    except Exception as e:
        logging.error(f"[validation_error] 无法读取请求体: {e}")
    
    return JSONResponse(
        status_code=422,
        content={
            "detail": "请求数据验证失败",
            "errors": exc.errors(),
            "body": body.decode('utf-8') if 'body' in locals() else "无法读取"
        }
    )


@app.on_event("startup")
async def startup_event():
    """FastAPI 启动时的事件处理"""
    logging.debug("[sidecar] FastAPI 应用启动中...")

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
logging.debug(f"[CORS] 配置的origins: {origins}")

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
        logging.debug(message)
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
    logging.debug(f"[sidecar] 准备启动API服务器")
    try:
        if server_instance is None:
            logging.debug(f"[sidecar] 正在启动API服务器...")
            config = Config(app, host="0.0.0.0", log_level="debug")
            server_instance = Server(config)
            # Start the ASGI server
            # Use a more robust approach to run the server
            try:
                logging.debug(f"[sidecar] 服务器配置完成，开始运行")
                server_instance.run()
            except Exception as e:
                logging.debug(f"[sidecar] API服务器运行错误: {e}")
        else:
            logging.debug(
                "[sidecar] 无法启动新服务器。服务器实例已在运行中。",
                flush=True,
            )
    except Exception as e:
        logging.debug(f"[sidecar] 错误，启动API服务器失败: {e}")


# Handle the stdin event loop. This can be used like a CLI.
def stdin_loop():
    logging.debug("[sidecar] Waiting for commands...")
    try:
        while True:
            # Read input from stdin.
            user_input = sys.stdin.readline().strip()

            # Check if the input matches one of the available functions
            match user_input:
                case "sidecar shutdown":
                    logging.debug("[sidecar] Received 'sidecar shutdown' command.")
                    kill_process()
                case _:
                    logging.debug(
                        f"[sidecar] Invalid command [{user_input}]. Try again."
                    )
    except Exception as e:
        logging.debug(f"[sidecar] stdin_loop error: {e}")


# Start the input loop in a separate thread
def start_input_thread():
    try:
        input_thread = threading.Thread(target=stdin_loop)
        input_thread.daemon = True  # so it exits when the main program exits
        input_thread.start()
    except Exception as e:
        logging.debug(f"[sidecar] Failed to start input handler: {e}")


if __name__ == "__main__":
    # Listen for stdin from parent process
    start_input_thread()

    # Starts API server, blocks further code from execution.
    try:
        start_api_server()
    except Exception as e:
        logging.debug(f"[sidecar] Fatal error in main thread: {e}")
        sys.exit(1)