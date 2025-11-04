Project Layout

- `resolve_api/` — Shared service package (FastAPI + shared DB access)
  - `resolve_api/main.py` — FastAPI entrypoint
  - `resolve_api/api/` — API routers and endpoints
    - `resolve_api/api/api.py` — Root router
    - `resolve_api/api/v1/endpoints/` — Versioned endpoints (e.g., `results.py`)
  - `resolve_api/core/` — Settings and DB engine
    - `resolve_api/core/config.py` — Pydantic settings from `.env`
    - `resolve_api/core/db_config.py` — SQLAlchemy engine/session factory
  - `resolve_api/schemas/` — Pydantic and SQLAlchemy schemas
    - `resolve_api/schemas/api.py` — Request/response models
    - `resolve_api/schemas/models.py` — SQLAlchemy models mapped to the Django DB
  - `resolve_api/services/` — Application services (DB write logic, orchestration)
    - `resolve_api/services/db_manager.py` — Persist GAP results into DB
  - `resolve_api/utils/` — Small reusable utilities
    - `resolve_api/utils/helpers.py` — Scenario logging via SQLAlchemy
    - `resolve_api/utils/utils.py` — Numeric helpers (e.g., large value handling)

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
- The API-side helpers and services were consolidated under `resolve_api/` to avoid duplication inside `src/`.
- Worker code remains under `worker/` and untouched in behavior.
- Prefer `resolve_api.utils.helpers.log_scenario` for API services; worker continues using its Django-based logging.
