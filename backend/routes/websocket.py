import os
import asyncio
import json
from fastapi import APIRouter, WebSocket, HTTPException
from fastapi.responses import FileResponse
from services.backend_service import get_watch_status

router = APIRouter()

# Global variables
watch_process = None
buffer_queue = None  # Will be set from main.py


def set_buffer_queue(queue):
    """Set the buffer queue from main.py"""
    global buffer_queue
    buffer_queue = queue


@router.websocket("/ws/changes")
async def websocket_changes(websocket: WebSocket):
    print(f"[ws/changes] New WebSocket connection from {websocket.client}")
    await websocket.accept()
    global buffer_queue
    try:
        while True:
            # 优先从缓冲队列读取
            try:
                if buffer_queue:
                    while not buffer_queue.empty():
                        data = buffer_queue.get_nowait()
                        await websocket.send_text(json.dumps(data, ensure_ascii=False))
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


@router.get("/api/watch/status")
async def api_get_watch_status():
    """Get the status of the fluctuation watch process"""
    global watch_process
    return get_watch_status(watch_process)


@router.get("/{rest_of_path:path}")
async def serve_frontend(rest_of_path: str):
    """Serve frontend files - this must be the last route"""
    dist_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "dist"))
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