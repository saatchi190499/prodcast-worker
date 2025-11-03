# src/services/scenario_status.py
from __future__ import annotations
import logging
from typing import Optional

from core.db_config import Session
from schemas.models import ScenarioClass

log = logging.getLogger(__name__)

def _normalize_scenario_id(sid: str) -> str:
    
    s = (sid or "").strip()
    if not s:
        return s
    up = s.upper()
    if up.startswith("SC"):
        return s
    if up.startswith(("RS", "GA")) and len(s) > 2:
        return "SC" + s[2:]
    if s.isdigit():
        return "SC" + s
    return s


def set_scenario_status(scenario_id: str, timestep: str, description: Optional[str] = None) -> bool:
    """
    Сохраняет в ScenarioClass.status значение вида "running_<дата>",
    где дата берётся из timestep ("timestep_03/01/2025" → "running_03/01/2025").
    """
    if not scenario_id:
        raise ValueError("scenario_id is required")
    if not timestep:
        raise ValueError("timestep is required")

    norm_id = _normalize_scenario_id(scenario_id)

    # извлечём дату из timestep
    if "_" in timestep:
        try:
            date_part = timestep.split("_", 1)[1]  # берём всё после "timestep_"
        except Exception:
            date_part = timestep
    else:
        date_part = timestep

    status_value = f"running_{date_part}"

    session = Session()
    try:
        rows = (
            session.query(ScenarioClass)
            .filter(ScenarioClass.scenario_id == norm_id)
            .update(
                {
                    ScenarioClass.status: status_value,
                    **({ScenarioClass.description: description} if description is not None else {}),
                },
                synchronize_session=False,
            )
        )
        session.commit()
        if rows == 0:
            log.warning("set_scenario_status: scenario %s (normalized %s) not found", scenario_id, norm_id)
            return False
        log.info("set_scenario_status: %s -> %s", norm_id, status_value)
        return True
    except Exception:
        log.exception("set_scenario_status failed for %s", norm_id)
        session.rollback()
        raise
    finally:
        session.close()