"""Results endpoints (moved from v1/endpoints)."""
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

from resolve_api.schemas.api import GapResults
from resolve_api.services.db_manager import delete_results_from_db, save_gap_results, update_scenario_status
from resolve_api.utils.helpers import log_scenario  # ⬅️ лог пишем напрямую в БД
from resolve_api.core.db_config import Session
from resolve_api.schemas.models import ScenarioClass as SA_Scenario

router = APIRouter()

@router.post("/gap_results")
async def retrieve_gap_results(data: GapResults):
    sid = data.scenario_id

    # Derive progress from current_timestep like "timestep_0" → 0
    def _parse_index(s: str) -> int:
        try:
            return int((s or "").split("_")[-1])
        except Exception:
            return 0

    # Base total from payload; user asked for "cout_timsteps"
    total_base = int(getattr(data, "cout_timsteps", 0) or 0)

    # Add months difference from scenario end_date - start_date
    month_add = 0
    session = Session()
    try:
        sc = session.query(SA_Scenario).filter(SA_Scenario.scenario_id == int(sid)).first()
        if sc and sc.start_date and sc.end_date and sc.end_date > sc.start_date:
            try:
                delta_days = (sc.end_date - sc.start_date).days
                month_add = max(0, delta_days // 30)
            except Exception:
                month_add = 0
    finally:
        session.close()

    total = total_base + month_add
    idx = _parse_index(getattr(data, "current_timestep", ""))
    if total > 0:
        ratio = (idx + 1) / total
        progress = int(round(ratio * 100))
        if progress < 0:
            progress = 0
        if progress > 100:
            progress = 100
    else:
        progress = 0

    log_scenario(sid, f"Incoming GAP results: timestep={data.timestep}", progress=progress)
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
        log_scenario(sid, f"GAP results saved at {data.timestep}", progress=progress)
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
