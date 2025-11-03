"""Main script"""

import uvicorn
from fastapi import FastAPI, Request
import time

from api.api import router
from core.config import settings
from utils.utils import get_api_logger, close_logger

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=settings.DESCRIPTION
)


# ── общий лог FastAPI: WorkServerLogs/_api/api.log
_api_logger, _api_handler = get_api_logger()
_api_logger.info("=== FastAPI service started ===")

@app.on_event("shutdown")
async def on_shutdown():
    _api_logger.info("=== FastAPI service stopping ===")
    close_logger(_api_logger, _api_handler)

# ── простая middleware для логирования всех HTTP-запросов
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    start = time.time()
    response = None
    try:
        response = await call_next(request)
        return response
    finally:
        dur_ms = int((time.time() - start) * 1000)
        status = getattr(response, "status_code", "n/a")
        _api_logger.info("%s %s -> %s (%d ms)", request.method, request.url.path, status, dur_ms)


app.include_router(router)

if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=settings.API_HOST,
        port=settings.API_PORT
    )
