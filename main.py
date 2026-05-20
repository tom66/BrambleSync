import os
import time
import uuid
import asyncio
from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global state for active jobs
jobs = {}

class FileOp(BaseModel):
    src: str
    dst: str

def get_optimal_block_size(file_size: int, path: str) -> int:
    """Calculate the best block size for I/O operations."""
    try:
        fs_block = os.stat(path).st_blksize
    except AttributeError:
        fs_block = 4096 # Fallback

    if file_size < 10 * 1024 * 1024:
        return max(fs_block * 16, 64 * 1024)
    elif file_size < 1024 * 1024 * 1024:
        return max(fs_block * 256, 1024 * 1024)
    else:
        return max(fs_block * 1024, 4 * 1024 * 1024)

async def chunked_copy(job_id: str, src: str, dst: str, remove_src: bool = False):
    """Async generator that copies a file in blocks, updating progress."""
    try:
        file_size = os.path.getsize(src)
        block_size = get_optimal_block_size(file_size, src)
        
        jobs[job_id] = {
            "file": os.path.basename(src),
            "progress": 0,
            "speed": 0,
            "status": "running"
        }

        start_time = time.time()
        copied = 0

        # Run file I/O in threadpool to prevent blocking the async loop
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            while True:
                # Read/Write chunks
                chunk = await asyncio.to_thread(fsrc.read, block_size)
                if not chunk:
                    break
                await asyncio.to_thread(fdst.write, chunk)
                
                copied += len(chunk)
                elapsed = time.time() - start_time
                speed_mb = (copied / elapsed) / (1024 * 1024) if elapsed > 0 else 0
                
                jobs[job_id]["progress"] = round((copied / file_size) * 100, 1) if file_size else 100
                jobs[job_id]["speed"] = round(speed_mb, 2)
                
                # Yield control back to event loop so API can serve progress requests
                await asyncio.sleep(0.005)

        if remove_src:
            os.remove(src)

        jobs[job_id]["status"] = "completed"
        jobs[job_id]["progress"] = 100
        
    except Exception as e:
        jobs[job_id]["status"] = f"error: {str(e)}"

@app.get("/")
def read_root():
    return FileResponse("static/index.html")

@app.get("/api/list")
def list_directory(path: str = "/"):
    """Returns contents of a directory."""
    if not os.path.exists(path) or not os.path.isdir(path):
        return JSONResponse({"error": "Invalid path"}, status_code=400)
    
    items = []
    # Always include parent dir if not root
    if path != "/":
        items.append({"name": "..", "is_dir": True, "path": os.path.dirname(path)})
        
    for entry in os.scandir(path):
        items.append({
            "name": entry.name,
            "is_dir": entry.is_dir(),
            "path": entry.path
        })
    # Sort: Directories first, then alphabetical
    items.sort(key=lambda x: (not x["is_dir"], x["name"].lower()))
    return {"path": path, "items": items}

@app.post("/api/copy")
async def api_copy(op: FileOp, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    background_tasks.add_task(chunked_copy, job_id, op.src, op.dst, False)
    return {"job_id": job_id}

@app.post("/api/move")
async def api_move(op: FileOp, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    # Try instant rename first (same filesystem)
    try:
        os.rename(op.src, op.dst)
        jobs[job_id] = {"file": os.path.basename(op.src), "progress": 100, "speed": 0, "status": "completed"}
    except OSError:
        # Cross-device link fallback (copy then delete)
        background_tasks.add_task(chunked_copy, job_id, op.src, op.dst, True)
    return {"job_id": job_id}

@app.post("/api/rename")
def api_rename(op: FileOp):
    try:
        os.rename(op.src, op.dst)
        return {"status": "success"}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.get("/api/jobs")
def get_jobs():
    """Returns active and completed jobs, cleans up completed ones."""
    current_jobs = dict(jobs)
    # Cleanup completed jobs so they don't bloat memory
    for k, v in list(jobs.items()):
        if v["status"] in ["completed", "error"]:
            del jobs[k]
    return current_jobs
