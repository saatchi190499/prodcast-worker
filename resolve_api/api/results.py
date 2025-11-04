"""Results endpoints (moved from v1/endpoints)."""
from fastapi import APIRouter, HTTPException
import re
from fastapi.responses import JSONResponse

from resolve_api.schemas.api import GapResults
from resolve_api.services.db_manager import delete_results_from_db, save_gap_results, update_scenario_status
from resolve_api.utils.helpers import log_scenario  # ⬅️ лог пишем напрямую в БД

router = APIRouter()

@router.post("/gap_results")
async def retrieve_gap_results(data: GapResults):
    sid = data.scenario_id

    # Derive progress from current_timestep (e.g., "timestep_0") and total timesteps
    def _parse_index(s: str) -> int:
        try:
            # Prefer suffix digits; fallback to split by underscore
            m = re.search(r"(\d+)$", s or "")
            if m:
                return int(m.group(1))
            parts = (s or "").split("_")
            return int(parts[-1]) if parts and parts[-1].isdigit() else 0
        except Exception:
            return 0

    total = int(getattr(data, "cout_timsteps", 0) or 0) + 2
    idx = _parse_index(getattr(data, "current_timestep", ""))
    if total > 0:
        ratio = (idx + 1) / total - 10/100
        progress = int(round(ratio * 100))
        if progress < 0:
            progress = 0
        if progress > 100:
            progress = 100
    else:
        progress = ""

    log_scenario(sid, f"Incoming GAP results at {data.timestep}", progress=progress)
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
        update_scenario_status(sid, data.timestep.split("_")[-1])
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
