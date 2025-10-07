import os
from celery import Celery
from kombu import Queue

app = Celery("prodcast_worker")

app.conf.broker_url = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
app.conf.result_backend = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")

# регистрируем очереди правильно
app.conf.task_queues = (
    Queue("scenarios"),
    Queue("workflows"),
)

# по умолчанию можно указать очередь
app.conf.task_default_queue = "scenarios"

# автопоиск задач в пакете worker
app.autodiscover_tasks(["worker"])
