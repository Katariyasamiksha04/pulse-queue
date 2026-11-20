FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true

COPY . .

EXPOSE 8080

CMD ["python", "-c", "from pulsequeue.server.http_server import PulseHTTPServer; from pulsequeue.core.queue import PriorityTaskQueue; from pulsequeue.core.dlq import DeadLetterQueue; from pulsequeue.worker.pool import WorkerPool; from pulsequeue.worker.executor import TaskExecutor; q = PriorityTaskQueue(); dlq = DeadLetterQueue(); pool = WorkerPool(q, TaskExecutor()); pool.start(); s = PulseHTTPServer(q, dlq, pool, host='0.0.0.0', port=8080); print('PulseQueue Server listening on port 8080...'); s.server.serve_forever()"]
