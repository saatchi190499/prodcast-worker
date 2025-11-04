"""Main script"""

import uvicorn
from fastapi import FastAPI

from app.api.api import router
from app.core.config import settings

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=settings.DESCRIPTION
)


@app.on_event("shutdown")
async def on_shutdown():
    # No logging on shutdown per requirement
    pass


app.include_router(router)

if __name__ == '__main__':
    uvicorn.run('app.main:app', host=settings.API_HOST, port=settings.API_PORT)
