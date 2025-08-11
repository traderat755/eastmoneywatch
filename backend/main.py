import os
import sys
import asyncio
import threading
import logging
import logging.handlers
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from uvicorn import Config, Server
from collections import deque
from datetime import datetime
from multiprocessing import Queue

from config import setup_logging
from services.backend_service import initialize_backend_services
from routes.api import router as api_router
from routes.websocket import router as websocket_router, set_buffer_queue

# 1. 设置日志
LOG_LEVEL = logging.DEBUG
log_queue, queue_listener = setup_logging(LOG_LEVEL)

# 2. 创建用于在后端和websockets之间传输数据的缓冲区队列
buffer_queue = Queue(maxsize=200)

# Global variables
server_instance = None
log_messages = deque(maxlen=1000)
active_websockets = set()
watch_process = None


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
    set_buffer_queue(buffer_queue)

    # 在后台线程中初始化服务, 并传递 buffer_queue 和 log_queue
    init_thread = threading.Thread(
        target=initialize_backend_services, 
        args=(buffer_queue, log_queue, LOG_LEVEL)  # Pass both queues
    )
    init_thread.daemon = True
    init_thread.start()

@app.on_event("shutdown")
async def shutdown_event():
    """FastAPI 关机时的事件处理"""
    logging.debug("[sidecar] FastAPI 应用正在关闭...")
    if queue_listener:
        queue_listener.stop()
        logging.debug("[sidecar] 日志队列监听器已停止。")


# Include routers
app.include_router(api_router)
app.include_router(websocket_router)

# 获取CORS origins配置
logging.debug(f"[CORS] 配置的origins: 允许所有内网请求")

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1|0\.0\.0\.0|10\.\d+.\d+.\d+|172\.(1[6-9]|2[0-9]|3[01])\.\d+\.\d+|192\.168\.\d+\.\d+)(:\d+)?|tauri://localhost",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def broadcast_message(message: str):
    """向所有连接的WebSocket客户端广播消息"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"[{timestamp}] {message}"
    log_messages.append(formatted_message)

    disconnected = set()
    for websocket in active_websockets:
        try:
            await websocket.send_text(formatted_message)
        except Exception:
            disconnected.add(websocket)
    active_websockets.difference_update(disconnected)


def output_reader(pipe, name):
    """从管道读取输出并打印 (现在由QueueListener处理，此函数可能不再需要)"""
    for line in pipe:
        message = f"[{name}] {line.strip()}"
        logging.debug(message)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(broadcast_message(message))
        loop.close()


def kill_process():
    global server_instance
    if server_instance is not None:
        server_instance.should_exit = True
    os._exit(0)


def start_api_server(**kwargs):
    global server_instance
    logging.debug(f"[sidecar] 准备启动API服务器")
    try:
        if server_instance is None:
            logging.debug(f"[sidecar] 正在启动API服务器...")
            config = Config(app, host="0.0.0.0", log_level="debug")
            server_instance = Server(config)
            logging.debug(f"[sidecar] 服务器配置完成，开始运行")
            server_instance.run()
        else:
            logging.debug("[sidecar] 服务器实例已在运行中。")
    except Exception as e:
        logging.error(f"[sidecar] 启动API服务器失败: {e}", exc_info=True)


def stdin_loop():
    logging.debug("[sidecar] Waiting for commands...")
    try:
        for line in sys.stdin:
            user_input = line.strip()
            if user_input == "sidecar shutdown":
                logging.debug("[sidecar] Received 'sidecar shutdown' command.")
                kill_process()
                break
            else:
                logging.debug(f"[sidecar] Invalid command [{user_input}]. Try again.")
    except Exception as e:
        logging.error(f"[sidecar] stdin_loop error: {e}", exc_info=True)


def start_input_thread():
    try:
        input_thread = threading.Thread(target=stdin_loop)
        input_thread.daemon = True
        input_thread.start()
    except Exception as e:
        logging.error(f"[sidecar] Failed to start input handler: {e}", exc_info=True)


if __name__ == "__main__":
    start_input_thread()
    try:
        start_api_server()
    except Exception as e:
        logging.critical(f"[sidecar] Fatal error in main thread: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # 确保listener在程序退出时被停止
        if queue_listener:
            queue_listener.stop()
