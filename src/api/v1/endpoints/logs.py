from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pathlib import Path
from core.config import settings

router = APIRouter()

# определяем базовую директорию логов один раз
PROJECT_ROOT = Path(__file__).resolve().parents[4]
LOGS_ROOT = PROJECT_ROOT / settings.WORK_LOGS_DIR  # "WorkServerLogs"

def _read_tail_bytes(path: Path, max_bytes: int) -> str:
    """
    Возвращает последние max_bytes байт файла path как текст.
    Сначала пробуем UTF-8, если не получилось — cp1251.
    """
    size = path.stat().st_size
    with path.open("rb") as f:
        if size > max_bytes:
            f.seek(size - max_bytes)
        data = f.read()

    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("cp1251", errors="replace")


@router.get("/logs/text", response_class=PlainTextResponse)
async def get_log_text(
    scenario_id: str,
    kind: str = Query(..., regex="^(worker|resolve|api)$"),
    tail: int = Query(6000, ge=100, le=2_000_000),
):
    """
    Вернуть хвост лога (worker / resolve / api) для заданного сценария.
    """
    base = LOGS_ROOT / scenario_id

    log_map = {
        "worker": base / "worker.log",
        "resolve": base / "resolve.log",
        "api": base / "api.log",
    }

    path = log_map[kind]

    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"log '{kind}' not found for {scenario_id} at {path}",
        )

    try:
        text = _read_tail_bytes(path, tail)
        return PlainTextResponse(text)
    except Exception as e:
        # если вдруг не смогли прочитать файл (права, блокировка и т.д.)
        raise HTTPException(status_code=500, detail=str(e))
