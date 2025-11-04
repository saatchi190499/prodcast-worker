"""Pydantic models for API requests"""

from pydantic import BaseModel


class GapResults(BaseModel):
    # values: str
    timestep: str
    scenario_id: int
    wells: str
    separators: str
    current_timestep: str
    str_gap_gor: str
    str_gap_gas_rate: str
    str_gap_oil_rate: str
    str_gap_drawdown: str
    str_gap_pres: str
    str_gap_wc: str
    str_gap_fwhp: str
    str_gap_pcontrol: str
    count_timsteps: int

