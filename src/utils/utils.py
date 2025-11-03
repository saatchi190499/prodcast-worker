"Utility functions for project"
# src/utils/utils.py
import logging
from pathlib import Path
from datetime import datetime
from core.config import settings
from core.db_config import Session
from schemas.models import ScenarioClass as SA_ScenarioClass  # noqa: F401 (kept for FK integrity)
from schemas.models import ScenarioLog as SA_ScenarioLog

def handle_large_values(value):
    try:
        float_value = float(value)
        if float_value > 1e+10:
            return 0
        return float_value
    except ValueError:
        return 0


def convert_input_data(
        keys: str,
        values: str,
        timestep: str
) -> dict[float | str]:
    keys_list = keys.split('|')
    values_list = [handle_large_values(item) for item in values.split('|')]
    timestep_conv = timestep.split('_')[1]
    result = {key: value for key, value in zip(keys_list, values_list)}
    result['time'] = timestep_conv
    
    return result

def _setup_file_logger(log_path: Path, name: str, mode: str = "a") -> tuple[logging.Logger, logging.Handler]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    fmt = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, mode=mode, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger, fh

def _logs_root() -> Path:
    """
    Возвращает абсолютный путь к каталогу логов.
    Если WORK_LOGS_DIR в .env задан абсолютным путём — используем его.
    Если относительным — привязываем к корню проекта (родитель папки src).
    """
    p = Path(settings.WORK_LOGS_DIR)
    if p.is_absolute():
        return p
    # файл сейчас: <project_root>/src/utils/utils.py
    project_root = Path(__file__).resolve().parents[2]  # подняться из utils -> src -> project_root
    return (project_root / p)

def get_api_logger() -> tuple[logging.Logger, logging.Handler]:
    """Общий лог FastAPI: WorkServerLogs/_api/api.log (рядом с src)"""
    path = _logs_root() / "_api" / "api.log"
    return _setup_file_logger(path, name="api", mode="a")

def get_scenario_api_logger(scenario_id: str) -> tuple[logging.Logger, logging.Handler]:
    """
    Scenario logger that writes ONLY to DB (apiapp_scenariolog).
    Returns (logger, handler) so caller can close the handler if desired.
    """
    logger = logging.getLogger(f"api.scenario.{scenario_id}")
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Resolve integer scenario_id (extract digits like SC123 -> 123)
    try:
        sc_digits = "".join(ch for ch in str(scenario_id) if ch.isdigit())
        sc_int = int(sc_digits) if sc_digits else None
    except Exception:
        sc_int = None

    class DBScenarioLogHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            if sc_int is None:
                return
            session = Session()
            try:
                progress = getattr(record, "progress", 0)
                session.add(
                    SA_ScenarioLog(
                        scenario_id=sc_int,
                        timestamp=datetime.utcnow(),
                        message=self.format(record) if self.formatter else record.getMessage(),
                        progress=int(progress) if isinstance(progress, (int, float)) else 0,
                    )
                )
                session.commit()
            except Exception:
                session.rollback()
            finally:
                session.close()

    dbh = DBScenarioLogHandler(level=logging.INFO)
    dbh.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(dbh)

    return logger, dbh

def sc_folder_id(sid) -> str:
    s = str(sid).strip()
    if len(s) >= 2:
        pfx = s[:2].upper()
        if pfx in ("GA", "RS"):
            return "SC" + s[2:]
        if pfx == "SC":
            return s
    return "SC" + s

def close_logger(logger: logging.Logger, handler: logging.Handler):
    try:
        logger.removeHandler(handler)
        handler.close()
    except Exception:
        pass
