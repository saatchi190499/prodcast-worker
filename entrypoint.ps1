$queues = $env:CELERY_QUEUES
if (-not $queues) { $queues = "scenarios" }

$pool = $env:CELERY_POOL
if (-not $pool) { $pool = "solo" }

$concurrency = $env:CELERY_CONCURRENCY
if (-not $concurrency) { $concurrency = "1" }

$name = $env:CELERY_NAME
if (-not $name) { $name = "worker@%h" }

python -m celery -A worker.celery worker -l info -Q $queues -P $pool --concurrency=$concurrency -n $name
