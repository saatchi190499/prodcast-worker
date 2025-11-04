Project structure and commands

- Celery workers:
  - `celery -A worker.celery worker -l info -Q scenarios --concurrency=1 -n scenario@%h`
  - `celery -A worker.celery worker -l info -Q workflows --concurrency=1 -n workflow@%h`
  - `celery -A worker.celery worker -l info -Q workflows -P solo -n workflow@%h`
  - `celery -A worker.celery worker -l info -Q scenarios -P solo -n scenario@%h`

- FastAPI service:
  - Entry: `resolve_api/main.py`
  - Run: `uvicorn resolve_api.main:app --host 0.0.0.0 --port 8080`
  - Run2: `python -m resolve_api.main`

See `docs/STRUCTURE.md` for a high-level layout and responsibilities.
