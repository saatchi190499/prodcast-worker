from fastapi import APIRouter

from api.v1.endpoints import results

router = APIRouter()
router.include_router(results.router, tags=['results'])
