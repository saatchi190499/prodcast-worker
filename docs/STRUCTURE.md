Project Layout

- `app/` — Shared service package (FastAPI + shared DB access)
  - `app/main.py` — FastAPI entrypoint
  - `app/api/` — API routers and endpoints
    - `app/api/api.py` — Root router
    - `app/api/v1/endpoints/` — Versioned endpoints (e.g., `results.py`)
  - `app/core/` — Settings and DB engine
    - `app/core/config.py` — Pydantic settings from `.env`
    - `app/core/db_config.py` — SQLAlchemy engine/session factory
  - `app/schemas/` — Pydantic and SQLAlchemy schemas
    - `app/schemas/api.py` — Request/response models
    - `app/schemas/models.py` — SQLAlchemy models mapped to the Django DB
  - `app/services/` — Application services (DB write logic, orchestration)
    - `app/services/db_manager.py` — Persist GAP results into DB
  - `app/utils/` — Small reusable utilities
    - `app/utils/helpers.py` — Scenario logging via SQLAlchemy
    - `app/utils/utils.py` — Numeric helpers (e.g., large value handling)

- `worker/` — Celery + Django ORM based worker
  - `worker/celery.py` — Celery app and queues
  - `worker/tasks.py` — Scenario and workflow tasks
  - `worker/models.py` — Django models targeting the same DB
  - `worker/helpers.py` — Worker-specific helpers (units, files, logging via Django)
  - `worker/db.py` — Minimal Django settings bootstrap
  - `worker/petex_client/` — Resolve/GAP automation client

- `requirements.txt` — Worker dependencies (Celery/Django). API deps may be managed separately.
- `.env` — Environment variables (DB connection, tokens, ports).

Notes

- The worker (Django ORM) and API (SQLAlchemy) both target the same database tables.
- The API-side helpers and services were consolidated under `app/` to avoid duplication inside `src/`.
- Worker code remains under `worker/` and untouched in behavior.
- Prefer `app.utils.helpers.log_scenario` for API services; worker continues using its Django-based logging.
