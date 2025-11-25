# distributed-task-queue
Distributed Task Queue &amp; Job Processor


# Distributed Task Queue & Job Processor

A distributed job queue system with worker pools, persistence, and real-time dashboard.

## Installation

### 1. Clone Repository
```bash
git clone https://github.com/SomalarajuRahul/distributed-task-queue.git
cd distributed_task_queue/backend
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\Activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## How to Start

### Start the Server
```bash
python -m uvicorn app.main:app --reload
```

Server runs on: `http://localhost:8000`

### Open Dashboard
```
http://localhost:8000/static/index.html
```

## Features

- Submit jobs via REST API
- Real-time job tracking (pending → running → done)
- Worker pool processes jobs with retry logic
- SQLite persistence (survives restarts)
- Dead Letter Queue for failed jobs
- Rate limiting (max 5 concurrent jobs per tenant)
- Live metrics dashboard
- System logs with trace IDs

## API Endpoints

### Submit Job
```bash
curl -X POST http://localhost:8000/api/jobs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: tenant-demo" \
  -d '{"payload": {"task": "process_data"}}'
```

### Get Job Status
```bash
curl http://localhost:8000/api/jobs/{job_id} \
  -H "X-Tenant-ID: tenant-demo"
```

### List Jobs
```bash
curl http://localhost:8000/api/jobs \
  -H "X-Tenant-ID: tenant-demo"
```

### Get Metrics
```bash
curl http://localhost:8000/api/metrics
```

## Project Structure
```
distributed_task_queue/
├── backend/
│   ├── venv/
│   ├── requirements.txt
│   └── app/
│       ├── main.py
│       └── static/
│           └── index.html
└── README.md
```

## How It Works

1. User submits job via API or dashboard
2. Job stored in SQLite database
3. Worker picks up pending job (lease for 30s)
4. Worker processes job and marks as done
5. If fails, job retries (max 3 times)
6. After max retries, job goes to Dead Letter Queue
7. Dashboard shows real-time status updates

## Design Trade-Offs

| Choice | Why | Trade-Off |
|--------|-----|-----------|
| **SQLite** | Simple, file-based, no server | Single machine only; use PostgreSQL for scale |
| **Polling Workers** | No message queue needed | Higher latency; use Redis/RabbitMQ for ~ms speed |
| **WebSocket Metrics** | Real-time dashboard | Doesn't scale; use Prometheus + Grafana for production |
| **Fixed Retry Logic** | Easy to understand | No exponential backoff; add jitter for large systems |

## Auto-Scale Workers (Conceptual)

How it would work in production:

```
1. Monitor queue depth: pending_jobs / running_workers
2. If queue_ratio > threshold (e.g., 5):
   - Spin up new worker pod (Kubernetes)
   - Scale up to max_workers (e.g., 10)
3. If queue_ratio < low_threshold (e.g., 1):
   - Shut down idle worker
   - Scale down to min_workers (e.g., 2)
4. Use metrics endpoint for monitoring
5. Update worker count dynamically via config

Implementation: Kubernetes HPA + custom metrics
or cloud functions (AWS Lambda, Google Cloud Functions)
```
