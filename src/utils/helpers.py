"""Helper utilities for DB-backed scenario logging and small helpers."""

from datetime import datetime

from core.db_config import Session
from schemas.models import ScenarioLog as SA_ScenarioLog


def log_scenario(scenario_id: int, message: str, progress: int | None = 0, ts: datetime | None = None) -> None:
    """Insert a log row into apiapp_scenariolog for the given scenario.

    - Extracts digits from `scenario_id` (e.g., "SC123" -> 123).
    - Writes `message` and optional `progress` and `timestamp`.
    """
    session = Session()
    try:
        session.add(
            SA_ScenarioLog(
                scenario_id=int(scenario_id),
                timestamp=ts or datetime.utcnow(),
                message=message,
                progress=int(progress or 0),
            )
        )
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
