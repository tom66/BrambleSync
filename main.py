import os
import time
import uuid
import asyncio
import math
from collections import deque
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global state
jobs = {}
active_files = set() # Tracks paths currently involved in operations

# Semaphore to limit concurrent operations to 3
concurrency_limit = None 

class FileOp(BaseModel):
    src: str
    dst: str

class MkdirOp(BaseModel):
    path: str

def check_and_lock(src: str, dst: str) -> bool:
    if src in active_files or dst in active_files:
        return False
    active_files.add(src)
    active_files.add(dst)
    return True

def unlock(src: str, dst: str):
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

def get_semaphore():
    """Lazily initialize the semaphore to ensure it attaches to the active event loop."""
    global concurrency_limit
    if concurrency_limit is None:
        concurrency_limit = asyncio.Semaphore(3)
    return concurrency_limit

async def execute_job(job_id: str):
    """The actual heavy lifting for I/O operations."""
    job = jobs[job_id]
    src, dst = job["src"], job["dst"]
    
    if job["type"] == "move":
        try:
            os.rename(src, dst)
            job["status"] = "completed"
            job["progress"] = 100.0
            job["speed"] = ""
            job["eta"] = ""
            return
        except OSError:
            job["remove_src"] = True 
            pass

    file_size = os.path.getsize(src)
    block_size = get_optimal_block_size(file_size, src)
    
    start_time = time.time()
    copied = 0
    
    # Track performance over the last 30 seconds
    history = deque([(start_time, 0)])
    last_append_time = start_time

    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        while True:
            if job.get("cancel"):
                if os.path.exists(dst):
                    os.remove(dst)
                job["status"] = "cancelled"
                job["progress"] = 0.0
                job["speed"] = ""
                job["eta"] = ""
                return

            chunk = await asyncio.to_thread(fsrc.read, block_size)
            if not chunk:
                break
            await asyncio.to_thread(fdst.write, chunk)
            
            copied += len(chunk)
            now = time.time()
            
            # Record a snapshot every 0.5s
            if now - last_append_time >= 0.5:
                history.append((now, copied))
                last_append_time = now
                
            # Discard snapshots older than 30 seconds
            while history and history[0][0] < now - 30.0:
                history.popleft()
                
            # Calculate rolling average
            window_time = now - history[0][0]
            window_bytes = copied - history[0][1]
            bytes_per_sec = window_bytes / window_time if window_time > 0 else 0
            
            # Format Speed
            if bytes_per_sec >= 1024 * 1024:
                job["speed"] = f"{bytes_per_sec / (1024 * 1024):.2f} MiB/s"
            else:
                job["speed"] = f"{bytes_per_sec / 1024:.2f} KiB/s"
                
            # Format ETA
            remaining_bytes = file_size - copied
            if bytes_per_sec > 0:
                eta_sec = int(remaining_bytes / bytes_per_sec)
                m, s = divmod(eta_sec, 60)
                job["eta"] = f"{m:02d}:{s:02d}"
            else:
                job["eta"] = "--:--"
            
            raw_percent = (copied / file_size) * 100 if file_size else 100.0
            job["progress"] = math.floor(raw_percent * 10) / 10.0
            
            await asyncio.sleep(0.005)

    if job.get("remove_src"):
        os.remove(src)

    job["status"] = "completed"
    job["progress"] = 100.0
    job["speed"] = ""
    job["eta"] = ""

async def process_job(job_id: str):
    """Waits for a slot, then executes the job."""
    job = jobs.get(job_id)
    if not job or job["status"] in ["cancelled", "error"]:
        return
        
    sem = get_semaphore()
    
    async with sem:
        if job["status"] == "cancelled":
            return
            
        job["status"] = "running"
        try:
            await execute_job(job_id)
        except Exception as e:
            job["status"] = f"error: {str(e)}"
        finally:
            unlock(job["src"], job["dst"])

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
    if op.src == op.dst:
        return JSONResponse({"error": "Source and destination are the same"}, status_code=400)
    if not check_and_lock(op.src, op.dst):
        return JSONResponse({"error": "File is currently in use by another operation"}, status_code=409)
    
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "type": "copy",
        "src": op.src,
        "dst": op.dst,
        "remove_src": False,
        "file": os.path.basename(op.src),
        "progress": 0.0,
        "speed": "",
        "eta": "",
        "status": "queued",
        "cancel": False
    }
    
    background_tasks.add_task(process_job, job_id)
    return {"job_id": job_id}

@app.post("/api/move")
async def api_move(op: FileOp, background_tasks: BackgroundTasks):
    if op.src == op.dst:
        return JSONResponse({"error": "Source and destination are the same"}, status_code=400)
    if not check_and_lock(op.src, op.dst):
        return JSONResponse({"error": "File is currently in use by another operation"}, status_code=409)
    
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "type": "move",
        "src": op.src,
        "dst": op.dst,
        "remove_src": False,
        "file": os.path.basename(op.src),
        "progress": 0.0,
        "speed": "",
        "eta": "",
        "status": "queued",
        "cancel": False
    }
    
    background_tasks.add_task(process_job, job_id)
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
    if job_id in jobs:
        if jobs[job_id]["status"] == "running":
            jobs[job_id]["cancel"] = True
            return {"status": "cancelling"}
        elif jobs[job_id]["status"] == "queued":
            jobs[job_id]["status"] = "cancelled"
            unlock(jobs[job_id]["src"], jobs[job_id]["dst"])
            return {"status": "cancelled"}
    return JSONResponse({"error": "Job not found or not active"}, status_code=400)

@app.get("/api/jobs")
def get_jobs():
    current_jobs = dict(jobs)
    for k, v in list(jobs.items()):
        if v["status"] in ["completed", "error", "cancelled"]:
            del jobs[k]
    return current_jobs
