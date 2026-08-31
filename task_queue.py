"""
Distributed Task Queue and Asynchronous Job Executor
Author: Rithamiga SP

Demonstrates:
- Async task queue with multiple worker coroutines
- Retry mechanism with exponential backoff
- Task states and results tracking
- Simple scheduling via priority queue
"""

import asyncio
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any


class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass(order=True)
class Task:
    priority: int
    id: str = field(compare=False, default_factory=lambda: str(uuid.uuid4())[:8])
    func: Callable = field(compare=False, default=None)
    args: tuple = field(compare=False, default_factory=tuple)
    max_retries: int = field(compare=False, default=3)
    attempts: int = field(compare=False, default=0)
    status: TaskStatus = field(compare=False, default=TaskStatus.PENDING)
    result: Any = field(compare=False, default=None)


class TaskQueue:
    def __init__(self, num_workers: int = 3):
        self.queue: asyncio.PriorityQueue[Task] = asyncio.PriorityQueue()
        self.num_workers = num_workers
        self.results: dict[str, Task] = {}

    def submit(self, func: Callable, *args, priority: int = 5, max_retries: int = 3) -> str:
        task = Task(priority=priority, func=func, args=args, max_retries=max_retries)
        self.results[task.id] = task
        self.queue.put_nowait(task)
        print(f"[submit] task={task.id} priority={priority} queued")
        return task.id

    async def _worker(self, worker_id: int):
        while True:
            task: Task = await self.queue.get()
            task.status = TaskStatus.RUNNING
            task.attempts += 1
            print(f"[worker-{worker_id}] running task={task.id} attempt={task.attempts}")

            try:
                task.result = await asyncio.to_thread(task.func, *task.args)
                task.status = TaskStatus.SUCCESS
                print(f"[worker-{worker_id}] task={task.id} SUCCESS result={task.result}")
            except Exception as e:
                if task.attempts < task.max_retries:
                    backoff = 2 ** task.attempts + random.random()
                    task.status = TaskStatus.RETRYING
                    print(f"[worker-{worker_id}] task={task.id} FAILED ({e}), retrying in {backoff:.1f}s")
                    await asyncio.sleep(backoff)
                    self.queue.put_nowait(task)
                else:
                    task.status = TaskStatus.FAILED
                    task.result = str(e)
                    print(f"[worker-{worker_id}] task={task.id} FAILED permanently after {task.attempts} attempts")
            finally:
                self.queue.task_done()

    async def run(self, duration: float = None):
        workers = [asyncio.create_task(self._worker(i)) for i in range(self.num_workers)]
        if duration:
            await asyncio.sleep(duration)
            for w in workers:
                w.cancel()
        else:
            await self.queue.join()
            for w in workers:
                w.cancel()

    def status_report(self):
        for task_id, task in self.results.items():
            print(f"task={task_id} status={task.status.value} attempts={task.attempts} result={task.result}")


# ---- Demo ----

def flaky_job(n: int) -> int:
    """Simulated job that sometimes fails, to demonstrate retries."""
    time.sleep(0.2)
    if random.random() < 0.4:
        raise RuntimeError(f"transient failure processing {n}")
    return n * n


async def main():
    tq = TaskQueue(num_workers=3)
    for i in range(8):
        tq.submit(flaky_job, i, priority=random.randint(1, 9))

    await tq.run()
    print("\n--- Final Status Report ---")
    tq.status_report()


if __name__ == "__main__":
    asyncio.run(main())
