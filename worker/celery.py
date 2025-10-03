import os
from celery import Celery

app = Celery("prodcast_worker")

app.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://redis:6379/0")
app.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://redis:6379/0")

# слушаем обе очереди
app.conf.task_queues = {
    "scenarios": {},
    "workflows": {},
}

app.autodiscover_tasks(["worker"])
