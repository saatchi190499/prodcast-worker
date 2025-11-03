from fastapi import APIRouter

from core.config import settings
from api.v1.endpoints import results, logs

router = APIRouter()
router.include_router(results.router, tags=['results'])
router.include_router(logs.router, tags=["logs"])