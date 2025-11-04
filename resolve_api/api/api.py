from fastapi import APIRouter

from resolve_api.api import results

router = APIRouter()
router.include_router(results.router, tags=['results'])
