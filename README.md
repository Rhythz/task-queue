# Distributed Task Queue and Asynchronous Job Executor

A Python framework for asynchronous task execution with worker scheduling, retry logic, and priority-based queue distribution.

## Features
- Multiple async worker coroutines pulling from a shared priority queue
- Automatic retry with exponential backoff on task failure
- Task status tracking (`pending`, `running`, `success`, `failed`, `retrying`)
- Priority scheduling — higher-priority tasks run first

## Run locally
```bash
python task_queue.py
```

You'll see worker logs showing tasks being picked up, occasionally failing and retrying, and a final status report.

## Extending this
- Swap `asyncio.PriorityQueue` for Redis/RabbitMQ to make it truly distributed across machines
- Add a persistent results store (SQLite/Postgres) instead of the in-memory dict
- Add a `/submit` and `/status/{task_id}` HTTP API on top using FastAPI
