"Utility functions for project"
# src/utils/utils.py
import logging
from pathlib import Path
from core.config import settings

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
    """Лог для конкретного сценария: WorkServerLogs/<scenario_id>/api.log (рядом с src)"""
    path = _logs_root() / str(scenario_id) / "api.log"
    return _setup_file_logger(path, name=f"api.scenario.{scenario_id}", mode="a")

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