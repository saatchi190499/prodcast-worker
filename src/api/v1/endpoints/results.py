# src/api/v1/endpoints/results.py
"""Endpoints for Resolve results"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from schemas.api import GapResults
from services.db_manager import delete_results_from_db, save_gap_results
from utils.utils import get_scenario_api_logger, close_logger, sc_folder_id  # ⬅️ добавили sc_folder_id

router = APIRouter()

@router.post("/gap_results")
async def retrieve_gap_results(data: GapResults):
    log_sid = sc_folder_id(data.scenario_id)

    logger, handler = get_scenario_api_logger(log_sid)
    logger.info(
        "Incoming /gap_results: scenario_id=%s (log:%s), timestep=%s",
        data.scenario_id, log_sid, data.timestep
    )
    try:
        # ⬇️ в БД исходный scenario_id
        save_gap_results(
            scenario_id=data.scenario_id,
            timestep=data.timestep,
            wells=data.wells,
            separators=data.separators,
            current_timestep=data.current_timestep,
            str_gap_gor=data.str_gap_gor,
            str_gap_gas_rate=data.str_gap_gas_rate,
            str_gap_oil_rate=data.str_gap_oil_rate,
            str_gap_drawdown=data.str_gap_drawdown,
            str_gap_pres=data.str_gap_pres,
            str_gap_wc=data.str_gap_wc,
            str_gap_fwhp=data.str_gap_fwhp,
            str_gap_pcontrol=data.str_gap_pcontrol,
        )
        logger.info("GAP saved OK: scenario_id=%s, timestep=%s", data.scenario_id, data.timestep)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.exception("Failed to save GAP results: %s", e)
        raise HTTPException(status_code=500, detail=f"GAP save failed: {e}")
    finally:
        close_logger(logger, handler)


@router.delete("/delete_gap_results")
async def delete_gap_results(scenario_id: int):
    log_sid = sc_folder_id(scenario_id)

    logger, handler = get_scenario_api_logger(log_sid)
    logger.info("Delete GA requested: scenario_id=%s (log:%s)", scenario_id, log_sid)
    try:
        delete_results_from_db(scenario_id)
        logger.info("Delete GA OK: scenario_id=%s", scenario_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.exception("Delete GA failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Delete GA failed: {e}")
    finally:
        close_logger(logger, handler)
