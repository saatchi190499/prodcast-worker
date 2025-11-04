# src/api/v1/endpoints/results.py
"""Endpoints for Resolve results"""

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from resolve_api.schemas.api import GapResults
from resolve_api.services.db_manager import delete_results_from_db, save_gap_results, update_scenario_status
from resolve_api.utils.helpers import log_scenario  # ⬅️ лог пишем напрямую в БД

router = APIRouter()

@router.post("/gap_results")
async def retrieve_gap_results(data: GapResults):
    sid = data.scenario_id
    log_scenario(sid, f"Incoming GAP results: timestep={data.timestep}")
    try:
        # ⬇️ в БД исходный scenario_id
        save_gap_results(
            scenario_id=sid,
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
        update_scenario_status(sid, data.timestep)
        log_scenario(sid, f"GAP results saved: timestep={data.timestep}")
        return JSONResponse({"ok": True})
    except Exception as e:
        log_scenario(sid, f"Failed to save GAP results: {e}")
        raise HTTPException(status_code=500, detail=f"GAP save failed: {e}")
    finally:
        pass


@router.delete("/delete_gap_results")
async def delete_gap_results(scenario_id: int):
    log_scenario(scenario_id, "Delete GAP results requested")
    try:
        delete_results_from_db(scenario_id)
        log_scenario(scenario_id, "GAP results deleted")
        return JSONResponse({"ok": True})
    except Exception as e:
        log_scenario(scenario_id, f"Delete GAP results failed: {e}")
        raise HTTPException(status_code=500, detail=f"Delete GA failed: {e}")
    finally:
        pass
