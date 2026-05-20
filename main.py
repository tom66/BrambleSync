import os
import time
import uuid
import asyncio
import math
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global state
jobs = {}
active_files = set() # Tracks paths currently involved in operations

class FileOp(BaseModel):
    src: str
    dst: str

class MkdirOp(BaseModel):
    path: str

def check_and_lock(src: str, dst: str) -> bool:
    """Check if files are busy, and lock them if not."""
    if src in active_files or dst in active_files:
        return False
    active_files.add(src)
    active_files.add(dst)
    return True

def unlock(src: str, dst: str):
    """Release the lock on files."""
    active_files.discard(src)
    active_files.discard(dst)

def get_optimal_block_size(file_size: int, path: str) -> int:
    try:
        fs_block = os.stat(path).st_blksize
    except AttributeError:
        fs_block = 4096

    if file_size < 10 * 1024 * 1024:
        return max(fs_block * 16, 64 * 1024)
    elif file_size < 1024 * 1024 * 1024:
        return max(fs_block * 256, 1024 * 1024)
    else:
        return max(fs_block * 1024, 4 * 1024 * 1024)

async def chunked_copy(job_id: str, src: str, dst: str, remove_src: bool = False):
    try:
        file_size = os.path.getsize(src)
        block_size = get_optimal_block_size(file_size, src)
        
        jobs[job_id] = {
            "file": os.path.basename(src),
            "progress": 0.0,
            "speed": 0,
            "status": "running",
            "cancel": False
        }

        start_time = time.time()
        copied = 0
        is_cancelled = False

        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            while True:
                if jobs[job_id].get("cancel"):
                    is_cancelled = True
                    break

                chunk = await asyncio.to_thread(fsrc.read, block_size)
                if not chunk:
                    break
                await asyncio.to_thread(fdst.write, chunk)
                
                copied += len(chunk)
                elapsed = time.time() - start_time
                speed_mb = (copied / elapsed) / (1024 * 1024) if elapsed > 0 else 0
                
                # Calculate percent and round down to 0.1 resolution
                raw_percent = (copied / file_size) * 100 if file_size else 100.0
                jobs[job_id]["progress"] = math.floor(raw_percent * 10) / 10.0
                jobs[job_id]["speed"] = round(speed_mb, 2)
                
                await asyncio.sleep(0.005)

        if is_cancelled:
            if os.path.exists(dst):
                os.remove(dst)
            jobs[job_id]["status"] = "cancelled"
            jobs[job_id]["progress"] = 0.0
            jobs[job_id]["speed"] = 0
            return

        if remove_src:
            os.remove(src)

        jobs[job_id]["status"] = "completed"
        jobs[job_id]["progress"] = 100.0
        jobs[job_id]["speed"] = 0
        
    except Exception as e:
        jobs[job_id]["status"] = f"error: {str(e)}"
    finally:
        unlock(src, dst)

@app.get("/")
def read_root():
    return FileResponse("static/index.html")

@app.get("/api/list")
def list_directory(path: str = "/"):
    if not os.path.exists(path) or not os.path.isdir(path):
        return JSONResponse({"error": "Invalid path"}, status_code=400)
    
    items = []
    if path != "/":
        items.append({"name": "..", "is_dir": True, "path": os.path.dirname(path), "size": 0})
        
    for entry in os.scandir(path):
        items.append({
            "name": entry.name,
            "is_dir": entry.is_dir(),
            "path": entry.path,
            "size": entry.stat().st_size if not entry.is_dir() else 0
        })
    items.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
    return {"path": path, "items": items}

@app.post("/api/mkdir")
def api_mkdir(op: MkdirOp):
    try:
        os.makedirs(op.path, exist_ok=False)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/copy")
async def api_copy(op: FileOp, background_tasks: BackgroundTasks):
    if not check_and_lock(op.src, op.dst):
        return JSONResponse({"error": "File is currently in use by another operation"}, status_code=409)
    
    job_id = str(uuid.uuid4())
    background_tasks.add_task(chunked_copy, job_id, op.src, op.dst, False)
    return {"job_id": job_id}

@app.post("/api/move")
async def api_move(op: FileOp, background_tasks: BackgroundTasks):
    if not check_and_lock(op.src, op.dst):
        return JSONResponse({"error": "File is currently in use by another operation"}, status_code=409)
    
    job_id = str(uuid.uuid4())
    try:
        os.rename(op.src, op.dst)
        jobs[job_id] = {"file": os.path.basename(op.src), "progress": 100.0, "speed": 0, "status": "completed"}
        unlock(op.src, op.dst) 
    except OSError:
        background_tasks.add_task(chunked_copy, job_id, op.src, op.dst, True)
    return {"job_id": job_id}

@app.post("/api/rename")
def api_rename(op: FileOp):
    if op.src in active_files or op.dst in active_files:
        return JSONResponse({"error": "File is currently in use by another operation"}, status_code=409)
    try:
        os.rename(op.src, op.dst)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/cancel/{job_id}")
def cancel_job(job_id: str):
    if job_id in jobs and jobs[job_id]["status"] == "running":
        jobs[job_id]["cancel"] = True
        return {"status": "cancelling"}
    return JSONResponse({"error": "Job not found or not running"}, status_code=400)

@app.get("/api/jobs")
def get_jobs():
    current_jobs = dict(jobs)
    for k, v in list(jobs.items()):
        if v["status"] in ["completed", "error", "cancelled"]:
            del jobs[k]
    return current_jobs
