import os
import asyncio
import json
import math
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
    
    # Helper function to clean NaN values from data before JSON serialization
    def clean_nan_values(obj):
        """Recursively clean NaN and None values from data structure"""
        if isinstance(obj, dict):
            return {k: clean_nan_values(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nan_values(item) for item in obj]
        elif isinstance(obj, float):
            # Check for NaN using multiple methods to be safe
            try:
                if math.isnan(obj) or obj != obj:
                    return 0  # Replace NaN with 0
                else:
                    return obj
            except (TypeError, ValueError):
                # If we can't check for NaN, treat it as a potential NaN
                return 0
        elif obj is None:
            return 0  # Replace None with 0 as well
        else:
            return obj
    
    try:
        while True:
            # 优先从缓冲队列读取
            try:
                if buffer_queue:
                    while not buffer_queue.empty():
                        data = buffer_queue.get_nowait()
                        # Handle NaN values by replacing them with 0 or null before JSON serialization
                        cleaned_data = clean_nan_values(data)
                        await websocket.send_text(json.dumps(cleaned_data, ensure_ascii=False))
            except Exception as e:
                error_msg = {"error": str(e)}
                # Ensure error messages don't contain NaN values either
                clean_error = clean_nan_values(error_msg)  
                await websocket.send_text(json.dumps(clean_error, ensure_ascii=False))
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