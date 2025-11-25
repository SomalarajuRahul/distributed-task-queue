"""
Distributed Task Queue & Job Processor
FastAPI backend with SQLite persistence, worker pool, and observability.
"""

import os
import json
import uuid
import logging
import asyncio
import hashlib
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Header, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import sqlite3
import uvicorn

# ============================================================================
# LOGGING & OBSERVABILITY
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


# ============================================================================
# DATABASE SETUP
# ============================================================================

DB_PATH = "distributed_task_queue.db"


def init_db():
    """Initialize SQLite database with schema."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Jobs table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            payload TEXT NOT NULL,
            idempotency_key TEXT,
            result TEXT,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            max_retries INTEGER DEFAULT 3,
            lease_until TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)
    
    # Metrics table for observability
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_type TEXT NOT NULL,
            tenant_id TEXT,
            value INTEGER DEFAULT 1,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Idempotency cache
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS idempotency_cache (
            idempotency_key TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("Database initialized")


class DB:
    """Thread-safe database wrapper."""
    
    @staticmethod
    def get_conn():
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        return conn
    
    @staticmethod
    def create_job(tenant_id: str, payload: Dict, idempotency_key: Optional[str] = None, 
                   max_retries: int = 3) -> str:
        """Create a new job. Returns job_id."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        # Check idempotency
        if idempotency_key:
            cursor.execute(
                "SELECT job_id FROM idempotency_cache WHERE idempotency_key = ? AND tenant_id = ?",
                (idempotency_key, tenant_id)
            )
            row = cursor.fetchone()
            if row:
                conn.close()
                logger.info(f"Idempotent request: {idempotency_key} -> job {row['job_id']}")
                return row['job_id']
        
        job_id = str(uuid.uuid4())
        cursor.execute(
            """INSERT INTO jobs 
               (id, tenant_id, status, payload, idempotency_key, max_retries)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (job_id, tenant_id, JobStatus.PENDING, json.dumps(payload), idempotency_key, max_retries)
        )
        
        if idempotency_key:
            cursor.execute(
                "INSERT INTO idempotency_cache (idempotency_key, tenant_id, job_id) VALUES (?, ?, ?)",
                (idempotency_key, tenant_id, job_id)
            )
        
        conn.commit()
        conn.close()
        logger.info(f"Job created: {job_id} by tenant {tenant_id} (trace_id={job_id})")
        return job_id
    
    @staticmethod
    def get_job(job_id: str) -> Optional[Dict]:
        """Get job details."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    
    @staticmethod
    def list_jobs(tenant_id: str, status: Optional[str] = None) -> list:
        """List jobs for a tenant."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        if status:
            cursor.execute(
                "SELECT * FROM jobs WHERE tenant_id = ? AND status = ? ORDER BY created_at DESC",
                (tenant_id, status)
            )
        else:
            cursor.execute(
                "SELECT * FROM jobs WHERE tenant_id = ? ORDER BY created_at DESC",
                (tenant_id,)
            )
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    @staticmethod
    def lease_job(job_id: str, lease_seconds: int = 30) -> bool:
        """Try to lease a job (mark as running with expiry)."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            """UPDATE jobs SET status = ?, lease_until = ?, started_at = CURRENT_TIMESTAMP
               WHERE id = ? AND status = ?""",
            (JobStatus.RUNNING, 
             datetime.utcnow() + timedelta(seconds=lease_seconds),
             job_id, JobStatus.PENDING)
        )
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        if success:
            logger.info(f"Job leased: {job_id} (trace_id={job_id})")
        
        return success
    
    @staticmethod
    def ack_job(job_id: str, result: Any):
        """Mark job as done."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE jobs SET status = ?, result = ?, completed_at = CURRENT_TIMESTAMP, lease_until = NULL
               WHERE id = ?""",
            (JobStatus.DONE, json.dumps(result), job_id)
        )
        conn.commit()
        conn.close()
        logger.info(f"Job completed: {job_id} (trace_id={job_id})")
    
    @staticmethod
    def fail_job(job_id: str, error: str) -> bool:
        """Fail a job. Returns True if should retry, False if to DLQ."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT retry_count, max_retries FROM jobs WHERE id = ?", (job_id,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return False
        
        retry_count = row['retry_count']
        max_retries = row['max_retries']
        
        if retry_count < max_retries:
            cursor.execute(
                """UPDATE jobs SET status = ?, retry_count = retry_count + 1, 
                   error_message = ?, lease_until = NULL WHERE id = ?""",
                (JobStatus.PENDING, error, job_id)
            )
            should_retry = True
            logger.info(f"Job retrying: {job_id} (retry {retry_count + 1}/{max_retries}) (trace_id={job_id})")
        else:
            cursor.execute(
                """UPDATE jobs SET status = ?, error_message = ?, lease_until = NULL WHERE id = ?""",
                (JobStatus.DEAD_LETTER, error, job_id)
            )
            should_retry = False
            logger.warning(f"Job to DLQ: {job_id} after {retry_count} retries (trace_id={job_id})")
        
        conn.commit()
        conn.close()
        return should_retry
    
    @staticmethod
    def get_pending_jobs(limit: int = 10) -> list:
        """Get pending jobs (for worker polling)."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        # Also check for expired leases
        cursor.execute(
            """SELECT * FROM jobs 
               WHERE (status = ? OR (status = ? AND lease_until < CURRENT_TIMESTAMP))
               ORDER BY created_at ASC LIMIT ?""",
            (JobStatus.PENDING, JobStatus.RUNNING, limit)
        )
        
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    
    @staticmethod
    def get_metrics() -> Dict:
        """Get observability metrics."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE status = ?", (JobStatus.PENDING,))
        pending = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE status = ?", (JobStatus.RUNNING,))
        running = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE status = ?", (JobStatus.DONE,))
        done = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE status = ?", (JobStatus.FAILED,))
        failed = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(*) as count FROM jobs WHERE status = ?", (JobStatus.DEAD_LETTER,))
        dlq = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(DISTINCT tenant_id) as count FROM jobs")
        tenants = cursor.fetchone()['count']
        
        conn.close()
        
        return {
            "pending": pending,
            "running": running,
            "done": done,
            "failed": failed,
            "dead_letter_queue": dlq,
            "active_tenants": tenants,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    @staticmethod
    def check_tenant_limits(tenant_id: str) -> Dict:
        """Check tenant quotas."""
        conn = DB.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT COUNT(*) as count FROM jobs WHERE tenant_id = ? AND status = ?",
            (tenant_id, JobStatus.RUNNING)
        )
        running = cursor.fetchone()['count']
        
        conn.close()
        return {"running": running, "max_concurrent": 5}


# ============================================================================
# WORKER POOL
# ============================================================================

class WorkerPool:
    """Simple worker pool that processes jobs."""
    
    def __init__(self, num_workers: int = 2):
        self.num_workers = num_workers
        self.running = False
    
    async def start(self):
        """Start worker tasks."""
        self.running = True
        logger.info(f"Starting {self.num_workers} workers")
        
        for i in range(self.num_workers):
            asyncio.create_task(self._worker_loop(i))
    
    async def _worker_loop(self, worker_id: int):
        """Main worker loop."""
        while self.running:
            try:
                jobs = DB.get_pending_jobs(limit=1)
                
                if not jobs:
                    await asyncio.sleep(1)
                    continue
                
                job = jobs[0]
                job_id = job['id']
                
                if not DB.lease_job(job_id):
                    continue
                
                try:
                    logger.info(f"Worker {worker_id} processing: {job_id}")
                    
                    # Simulate job processing
                    payload = json.loads(job['payload'])
                    await asyncio.sleep(0.5)  # Simulate work
                    
                    result = {
                        "processed": payload,
                        "worker": worker_id,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
                    DB.ack_job(job_id, result)
                    logger.info(f"Worker {worker_id} completed: {job_id}")
                    
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Worker {worker_id} error on {job_id}: {error_msg}")
                    DB.fail_job(job_id, error_msg)
            
            except Exception as e:
                logger.error(f"Worker {worker_id} fatal error: {e}")
                await asyncio.sleep(1)


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

# Request/Response models
class SubmitJobRequest(BaseModel):
    payload: Dict[str, Any]
    idempotency_key: Optional[str] = None
    max_retries: int = 3


class JobResponse(BaseModel):
    id: str
    status: str
    payload: Dict
    result: Optional[Dict] = None
    error_message: Optional[str] = None
    retry_count: int
    created_at: str


class MetricsResponse(BaseModel):
    pending: int
    running: int
    done: int
    failed: int
    dead_letter_queue: int
    active_tenants: int
    timestamp: str


# Global state
worker_pool = WorkerPool(num_workers=2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB and start workers on app startup."""
    init_db()
    await worker_pool.start()
    yield
    worker_pool.running = False


app = FastAPI(title="Distributed Task Queue", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
if os.path.exists("app/static"):
    app.mount("/static", StaticFiles(directory="app/static", html=True), name="static")


# ============================================================================
# API ENDPOINTS
# ============================================================================

def get_tenant_id(x_tenant_id: str = Header(...)) -> str:
    """Extract and validate tenant ID from headers."""
    if not x_tenant_id:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-ID header")
    return x_tenant_id


@app.post("/api/jobs", response_model=Dict)
async def submit_job(
    request: SubmitJobRequest,
    tenant_id: str = Header(..., alias="X-Tenant-ID")
):
    """Submit a new job."""
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Missing X-Tenant-ID header")
    
    # Check tenant limits
    limits = DB.check_tenant_limits(tenant_id)
    if limits["running"] >= limits["max_concurrent"]:
        raise HTTPException(
            status_code=429,
            detail=f"Tenant has max {limits['max_concurrent']} concurrent jobs"
        )
    
    job_id = DB.create_job(
        tenant_id=tenant_id,
        payload=request.payload,
        idempotency_key=request.idempotency_key,
        max_retries=request.max_retries
    )
    
    return {"job_id": job_id, "status": JobStatus.PENDING}


@app.get("/api/jobs/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    tenant_id: str = Header(..., alias="X-Tenant-ID")
):
    """Get job status."""
    job = DB.get_job(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    if job['tenant_id'] != tenant_id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    return JobResponse(
        id=job['id'],
        status=job['status'],
        payload=json.loads(job['payload']),
        result=json.loads(job['result']) if job['result'] else None,
        error_message=job['error_message'],
        retry_count=job['retry_count'],
        created_at=job['created_at']
    )


@app.get("/api/jobs", response_model=Dict)
async def list_jobs(
    tenant_id: str = Header(..., alias="X-Tenant-ID"),
    status: Optional[str] = None
):
    """List tenant's jobs."""
    jobs = DB.list_jobs(tenant_id, status)
    return {
        "jobs": [
            JobResponse(
                id=j['id'],
                status=j['status'],
                payload=json.loads(j['payload']),
                result=json.loads(j['result']) if j['result'] else None,
                error_message=j['error_message'],
                retry_count=j['retry_count'],
                created_at=j['created_at']
            ).dict()
            for j in jobs
        ],
        "count": len(jobs)
    }


@app.get("/api/metrics", response_model=MetricsResponse)
async def get_metrics():
    """Get system metrics."""
    return MetricsResponse(**DB.get_metrics())


@app.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    """WebSocket for real-time metrics."""
    await websocket.accept()
    try:
        while True:
            metrics = DB.get_metrics()
            await websocket.send_json(metrics)
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("Metrics WebSocket disconnected")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    init_db()
    uvicorn.run(app, host="0.0.0.0", port=8000)