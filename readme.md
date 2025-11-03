celery -A worker.celery worker -l info -Q scenarios --concurrency=1 -n scenario@%h
celery -A worker.celery worker -l info -Q workflows --concurrency=1 -n workflow@%h

celery -A worker.celery worker -l info -Q workflows -P solo -n workflow@%h
celery -A worker.celery worker -l info -Q scenarios -P solo -n scenario@%h
