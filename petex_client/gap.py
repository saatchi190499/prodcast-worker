"""
Wrappers for GAP OpenServer functions (Petroleum Experts IPM) based on the
OpenServer User's Manual (GAP section). Each wrapper accepts a PetexServer
instance and issues the appropriate OpenServer command/string.

Assumptions:
- PetexServer exposes:
    - do_cmd(cmd: str) -> None        # Execute an OpenServer command
    - get_value(tag: str) -> Any      # Read a value (e.g., GAP.LASTCMDRET)
    - set_value(tag: str, val: Any)   # Set a tag value

Labels & Indexes:
- Functions that accept a model/equipment/well/layer identifier allow either an
  integer index or a string label. String labels are wrapped as {LABEL} to match
  OpenServer's label addressing, while ints are used verbatim. Values that look
  like UniqueIDs (e.g., 'ID657D653E') are passed as-is (no braces).

Return values:
- Where GAP functions return a value, we issue the command and then read
  GAP.LASTCMDRET (converted to int/float/str as convenient).

Usage example:
    from gap_openserver import open_well, solve_network, pred_init

    open_well(srv, model="PROD", well="W1")
    solve_network(srv)
    nsteps = pred_init(srv)
"""

from __future__ import annotations
from typing import Any, Optional, Union, List, Iterable
from fnmatch import fnmatch
from .utils import split_gap_list

IdxOrLabel = Union[int, str]

# -------------------------------
# Internal helpers
# -------------------------------

def _b(flag: bool) -> str:
    """
    Convert a Python boolean to the OpenServer literal "TRUE"/"FALSE".

    Parameters:
        flag (bool): Python boolean.

    Returns:
        str: "TRUE" if flag is True, else "FALSE".
    """
    return "TRUE" if flag else "FALSE"


def _q(s: str) -> str:
    """
    Quote a string for OpenServer function calls, escaping internal quotes.

    Parameters:
        s (str): Raw string.

    Returns:
        str: String wrapped in double quotes with `"` escaped.
    """
    return '"' + str(s).replace('"', '\\"') + '"'


def _wrap_idx_or_label(x: IdxOrLabel) -> str:
    """
    Normalize an equipment/model identifier to an OpenServer selector.

    Normalization rules:
      - int -> "3" (index form)
      - str that looks like a UniqueID (starts with 'ID' + hex chars) -> passed as-is
      - any other str -> wrapped as {Label}

    Parameters:
        x (int | str): Index, label, or UniqueID.

    Returns:
        str: Selector suitable for GAP.MOD[...] addressing.
    """
    if isinstance(x, int):
        return str(x)
    s = str(x)
    if s.startswith("ID") and all(c in "0123456789ABCDEF" for c in s[2:]):
        return s  # UniqueID: no braces
    return "{" + s + "}"


def _last_ret(srv) -> Optional[str]:
    """
    Read GAP.LASTCMDRET after a command that returns a value.

    Parameters:
        srv: PetexServer implementation.

    Returns:
        Optional[str]: Raw LASTCMDRET value as string or None if unavailable.
    """
    try:
        return srv.get_value("GAP.LASTCMDRET")
    except Exception:
        return None


def _last_ret_int(srv) -> Optional[int]:
    """
    Convert GAP.LASTCMDRET to int where possible.

    Parameters:
        srv: PetexServer implementation.

    Returns:
        Optional[int]: Parsed integer or None on failure/empty.
    """
    r = _last_ret(srv)
    try:
        return int(str(r).strip()) if r is not None and str(r).strip() != "" else None
    except Exception:
        return None


def _last_ret_float(srv) -> Optional[float]:
    """
    Convert GAP.LASTCMDRET to float where possible.

    Parameters:
        srv: PetexServer implementation.

    Returns:
        Optional[float]: Parsed float or None on failure/empty.
    """
    r = _last_ret(srv)
    try:
        return float(str(r).strip()) if r is not None and str(r).strip() != "" else None
    except Exception:
        return None


# ============================================================
# Network / General GAP functions
# ============================================================

def calc_flow_assurance(srv) -> None:
    """
    Run the Flow Assurance calculation.

    Issues:
        GAP.CALCFLOWASSURANCE()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.CALCFLOWASSURANCE()")


def calc_gradient(srv) -> None:
    """
    Recalculate gradients across the network.

    Issues:
        GAP.CALCGRADIENT()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.CALCGRADIENT()")


def calc_comp_dp(srv, comp: IdxOrLabel) -> None:
    """
    Calculate differential pressure for a compressor.

    Issues:
        GAP.CALCCOMPDP(comp)

    Parameters:
        srv: PetexServer.
        comp (IdxOrLabel): Compressor index/label/UniqueID.
    """
    srv.do_cmd(f"GAP.CALCCOMPDP({_wrap_idx_or_label(comp)})")


def calc_pump_dp(srv, pump: IdxOrLabel) -> None:
    """
    Calculate differential pressure for a pump.

    Issues:
        GAP.CALCPUMPDP(pump)

    Parameters:
        srv: PetexServer.
        pump (IdxOrLabel): Pump index/label/UniqueID.
    """
    srv.do_cmd(f"GAP.CALCPUMPDP({_wrap_idx_or_label(pump)})")


def calc_pipe_dp(srv, pipe: IdxOrLabel) -> None:
    """
    Calculate differential pressure for a pipe.

    Issues:
        GAP.CALCPIPEDP(pipe)

    Parameters:
        srv: PetexServer.
        pipe (IdxOrLabel): Pipe index/label/UniqueID.
    """
    srv.do_cmd(f"GAP.CALCPIPEDP({_wrap_idx_or_label(pipe)})")


def copy_item(srv, equip_dest: IdxOrLabel, equip_src: IdxOrLabel) -> None:
    """
    Copy a network item into another.

    Issues:
        GAP.COPYITEM(EquipDest, EquipSrc)

    Parameters:
        srv: PetexServer.
        equip_dest (IdxOrLabel): Destination equipment.
        equip_src (IdxOrLabel): Source equipment to copy from.
    """
    srv.do_cmd(f"GAP.COPYITEM({_wrap_idx_or_label(equip_dest)}, {_wrap_idx_or_label(equip_src)})")


def del_item(srv, node: IdxOrLabel) -> None:
    """
    Delete an item (equipment/node) from the model.

    Issues:
        GAP.DELITEM(node)

    Parameters:
        srv: PetexServer.
        node (IdxOrLabel): Target equipment/node.
    """
    srv.do_cmd(f"GAP.DELITEM({_wrap_idx_or_label(node)})")


def is_member_of_group(srv, group: str) -> Optional[int]:
    """
    Test whether the current user/session is a member of a GAP security group.

    Issues:
        GAP.ISMEMBEROFGROUP("Group")

    Parameters:
        srv: PetexServer.
        group (str): Group name.

    Returns:
        Optional[int]: 1 if member, 0 if not, None on failure.
    """
    srv.do_cmd(f"GAP.ISMEMBEROFGROUP({_q(group)})")
    return _last_ret_int(srv)


def link_items(srv, equip1: IdxOrLabel, equip2: IdxOrLabel, linklabel: str = "") -> None:
    """
    Link two items (default ports) with an optional link label.

    Issues:
        GAP.LINKITEMS(equip1, equip2, "linklabel")

    Parameters:
        srv: PetexServer.
        equip1 (IdxOrLabel): First item.
        equip2 (IdxOrLabel): Second item.
        linklabel (str, optional): Visual/metadata label for the link.
    """
    srv.do_cmd(
        f"GAP.LINKITEMS({_wrap_idx_or_label(equip1)}, {_wrap_idx_or_label(equip2)}, {_q(linklabel)})"
    )


def link_item_ports(
    srv,
    equip1: IdxOrLabel,
    port_desc1: str,
    equip2: IdxOrLabel,
    port_desc2: str,
    linklabel: str = "",
) -> None:
    """
    Link two items by explicit port descriptors.

    Issues:
        GAP.LINKITEMPORTS(equip1, "port1", equip2, "port2", "linklabel")

    Parameters:
        srv: PetexServer.
        equip1 (IdxOrLabel): First item.
        port_desc1 (str): Port descriptor on first item.
        equip2 (IdxOrLabel): Second item.
        port_desc2 (str): Port descriptor on second item.
        linklabel (str, optional): Link label.
    """
    srv.do_cmd(
        "GAP.LINKITEMPORTS("
        f"{_wrap_idx_or_label(equip1)}, {_q(port_desc1)}, "
        f"{_wrap_idx_or_label(equip2)}, {_q(port_desc2)}, {_q(linklabel)})"
    )


def new_file(srv) -> None:
    """
    Create a new GAP file (clear model).

    Issues:
        GAP.NEWFILE()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.NEWFILE()")


def new_item(srv, type_name: str, label: str, iposcode: int, equip: IdxOrLabel, model: IdxOrLabel) -> None:
    """
    Create a new item in the network.

    Issues:
        GAP.NEWITEM(type, label, iposcode, equip, model)

    Parameters:
        srv: PetexServer.
        type_name (str): Equipment type name (e.g., 'WELL', 'JOINT').
        label (str): Visible label.
        iposcode (int): Position code (UI layout hint).
        equip (IdxOrLabel): Parent/related equipment (context-dependent).
        model (IdxOrLabel): Model selector (e.g., 0 or "PROD").
    """
    srv.do_cmd(
        f"GAP.NEWITEM({_q(type_name)}, {_q(label)}, {int(iposcode)}, "
        f"{_wrap_idx_or_label(equip)}, {_wrap_idx_or_label(model)})"
    )


def start(srv):
    """
    Start GAP application (if supported in your automation context).

    Issues:
        GAP.Start

    Parameters:
        srv: PetexServer.

    Returns:
        Any: Whatever srv.do_cmd returns (usually None).
    """
    return srv.do_cmd("GAP.Start")


def open_file(srv, filename: str) -> None:
    """
    Open an existing GAP model file.

    Issues:
        GAP.OPENFILE("path")

    Parameters:
        srv: PetexServer.
        filename (str): Full path to .gap/.gar as applicable.
    """
    srv.do_cmd(f"GAP.OPENFILE({_q(filename)})")


def save_file(srv, filename: str) -> None:
    """
    Save the current GAP model to a file.

    Issues:
        GAP.SAVEFILE("path")

    Parameters:
        srv: PetexServer.
        filename (str): Destination file path.
    """
    srv.do_cmd(f"GAP.SAVEFILE({_q(filename)})")


def shutdown(srv, save: bool = True) -> None:
    """
    Shut down GAP, optionally saving first.

    Parameters:
        srv: PetexServer.
        save (bool): If True, save before closing.
    """
    arg = 1 if save else 0
    srv.do_cmd(f"GAP.SHUTDOWN({arg})")


def solve_network(srv) -> None:
    """
    Solve the current network.

    Issues:
        GAP.SOLVENETWORK()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.SOLVENETWORK()")


def tpd_calc(srv) -> None:
    """
    Recalculate tubing performance / TPD.

    Issues:
        GAP.TPD.CALC()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.TPD.CALC()")


def transfer_prosper_ipr(srv, well: IdxOrLabel, layernumber: int, pvt_method: int) -> None:
    """
    Transfer PROSPER IPR into GAP for a given well/layer.

    Issues:
        GAP.TRANSFERPROSPERIPR(well, layernumber, PVTMethod)

    Parameters:
        srv: PetexServer.
        well (IdxOrLabel): Target well.
        layernumber (int): IPR layer number.
        pvt_method (int): PVT method index/key expected by GAP.
    """
    srv.do_cmd(
        f"GAP.TRANSFERPROSPERIPR({_wrap_idx_or_label(well)}, {int(layernumber)}, {int(pvt_method)})"
    )


def unlink_items(srv, equip1: IdxOrLabel, equip2: IdxOrLabel) -> None:
    """
    Remove a link between two items.

    Issues:
        GAP.UNLINKITEMS(equip1, equip2)

    Parameters:
        srv: PetexServer.
        equip1 (IdxOrLabel): First item.
        equip2 (IdxOrLabel): Second item.
    """
    srv.do_cmd(f"GAP.UNLINKITEMS({_wrap_idx_or_label(equip1)}, {_wrap_idx_or_label(equip2)})")


def validate(srv, solver_or_pred: int) -> None:
    """
    Validate model at network level.

    Issues:
        GAP.VALIDATE(solverorpred)

    Parameters:
        srv: PetexServer.
        solver_or_pred (int): Mode per manual (e.g., 0=Solver, 1=Prediction).
    """
    srv.do_cmd(f"GAP.VALIDATE({int(solver_or_pred)})")


def vlp_import(srv, equip: IdxOrLabel, filename: str) -> None:
    """
    Import a VLP table for an equipment.

    Issues:
        GAP.VLPIMPORT(equip, "filename")

    Parameters:
        srv: PetexServer.
        equip (IdxOrLabel): Equipment to receive the VLP.
        filename (str): Path to VLP file.
    """
    srv.do_cmd(f"GAP.VLPIMPORT({_wrap_idx_or_label(equip)}, {_q(filename)})")


def vlp_ipr_pc_gen(srv, well: IdxOrLabel, autowhp: bool) -> None:
    """
    Generate VLP+IPR PC (performance curve) for a well.

    Issues:
        GAP.VLPIPRPCGEN(well, autowhp)

    Parameters:
        srv: PetexServer.
        well (IdxOrLabel): Well selector.
        autowhp (bool): If True, WHP is auto-selected during generation.
    """
    srv.do_cmd(f"GAP.VLPIPRPCGEN({_wrap_idx_or_label(well)}, {_b(autowhp)})")


def well_calc(srv, well: IdxOrLabel) -> None:
    """
    Recalculate a well (local calculation).

    Issues:
        GAP.WELLCALC(well)

    Parameters:
        srv: PetexServer.
        well (IdxOrLabel): Well selector.
    """
    srv.do_cmd(f"GAP.WELLCALC({_wrap_idx_or_label(well)})")


# ============================================================
# Prediction / Solver functions
# ============================================================

def pred_init(
    srv,
    ignore_internal_timestep: bool = False,
    ignore_internal_scheduling: bool = False,
) -> Optional[int]:
    """
    Initialize prediction run and return the number of steps.

    Issues:
        GAP.PREDINIT(ignore_internal_timestep, ignore_internal_scheduling)

    Parameters:
        srv: PetexServer.
        ignore_internal_timestep (bool): If True, overrides internal timestep logic.
        ignore_internal_scheduling (bool): If True, ignores internal scheduling.

    Returns:
        Optional[int]: Number of steps (LASTCMDRET) or None if unavailable.
    """
    srv.do_cmd(f"GAP.PREDINIT({_b(ignore_internal_timestep)}, {_b(ignore_internal_scheduling)})")
    return _last_ret_int(srv)


def pred_do_step(srv, optimise: bool, potential: bool) -> None:
    """
    Execute a single prediction step.

    Issues:
        GAP.PREDDOSTEP(optimise, potential)

    Parameters:
        srv: PetexServer.
        optimise (bool): Enable optimizer during step.
        potential (bool): Keep potential wells/elements active.
    """
    srv.do_cmd(f"GAP.PREDDOSTEP({_b(optimise)}, {_b(potential)})")


def pred_do_solver(srv, timestepsize: float, optimise: bool, potential: bool, model: IdxOrLabel) -> None:
    """
    Run the solver for a given timestep in prediction mode.

    Issues:
        GAP.PREDDOSOLVER(timestepsize, optimise, potential, model)

    Parameters:
        srv: PetexServer.
        timestepsize (float): Step duration (units per model setting).
        optimise (bool): Enable optimizer.
        potential (bool): Keep potential items.
        model (IdxOrLabel): Model selector (index/label).
    """
    srv.do_cmd(
        f"GAP.PREDDOSOLVER({float(timestepsize)}, {_b(optimise)}, {_b(potential)}, {_wrap_idx_or_label(model)})"
    )


def pred_end(srv, dorest: bool, optimise: bool, potential: bool) -> None:
    """
    Finalize prediction run.

    Issues:
        GAP.PREDEND(dorest, optimise, potential)

    Parameters:
        srv: PetexServer.
        dorest (bool): Perform restoration at end.
        optimise (bool): Enable optimizer at finalization.
        potential (bool): Keep potential items at finalization.
    """
    srv.do_cmd(f"GAP.PREDEND({_b(dorest)}, {_b(optimise)}, {_b(potential)})")


def purge_all_results(srv, model: IdxOrLabel) -> None:
    """
    Purge all results for the specified model.

    Issues:
        GAP.PURGEALLRESULTS(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGEALLRESULTS({_wrap_idx_or_label(model)})")


def purge_pred_log(srv, model: IdxOrLabel) -> None:
    """
    Purge prediction logs for the model.

    Issues:
        GAP.PURGEPREDLOG(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGEPREDLOG({_wrap_idx_or_label(model)})")


def purge_pred_results(srv, model: IdxOrLabel) -> None:
    """
    Purge prediction results for the model.

    Issues:
        GAP.PURGEPREDRESULTS(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGEPREDRESULTS({_wrap_idx_or_label(model)})")


def purge_pred_snapshot(srv, model: IdxOrLabel) -> None:
    """
    Purge prediction snapshots for the model.

    Issues:
        GAP.PURGEPREDSNAPSHOT(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGEPREDSNAPSHOT({_wrap_idx_or_label(model)})")


def purge_solver_log(srv, model: IdxOrLabel) -> None:
    """
    Purge solver logs for the model.

    Issues:
        GAP.PURGESOLVERLOG(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGESOLVERLOG({_wrap_idx_or_label(model)})")


def purge_solver_results(srv, model: IdxOrLabel) -> None:
    """
    Purge solver results for the model.

    Issues:
        GAP.PURGESOLVERRESULTS(model)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    srv.do_cmd(f"GAP.PURGESOLVERRESULTS({_wrap_idx_or_label(model)})")


def refit_pc(srv, well: IdxOrLabel) -> None:
    """
    Refit performance curves (PC) for a well.

    Issues:
        GAP.REFITPC(well)

    Parameters:
        srv: PetexServer.
        well (IdxOrLabel): Well selector.
    """
    srv.do_cmd(f"GAP.REFITPC({_wrap_idx_or_label(well)})")


def reset_solver_inputs(srv) -> None:
    """
    Reset solver inputs to defaults/initial state.

    Issues:
        GAP.RESETSOLVERINPUTS()

    Parameters:
        srv: PetexServer.
    """
    srv.do_cmd("GAP.RESETSOLVERINPUTS()")


# ============================================================
# Performance Curve functions
# ============================================================

def pc_calc(srv, equip: IdxOrLabel, pc_curve: int) -> None:
    """
    Calculate/update a performance curve for an equipment.

    Issues:
        GAP.PCCALC(equip, PcCurve)

    Parameters:
        srv: PetexServer.
        equip (IdxOrLabel): Equipment selector.
        pc_curve (int): Curve index per model definition.
    """
    srv.do_cmd(f"GAP.PCCALC({_wrap_idx_or_label(equip)}, {int(pc_curve)})")


def pc_gmax(srv, equip: IdxOrLabel, pc_curve: int) -> None:
    """
    Run GMAX calculation for a PC curve.

    Issues:
        GAP.PCGMAX(equip, PcCurve)

    Parameters:
        srv: PetexServer.
        equip (IdxOrLabel): Equipment selector.
        pc_curve (int): Curve index per model definition.
    """
    srv.do_cmd(f"GAP.PCGMAX({_wrap_idx_or_label(equip)}, {int(pc_curve)})")


def pc_gsolve(srv, equip: IdxOrLabel, pc_curve: int) -> None:
    """
    Solve gas-related PC curve values.

    Issues:
        GAP.PCGSOLV(equip, PcCurve)

    Parameters:
        srv: PetexServer.
        equip (IdxOrLabel): Equipment selector.
        pc_curve (int): Curve index per model definition.
    """
    srv.do_cmd(f"GAP.PCGSOLV({_wrap_idx_or_label(equip)}, {int(pc_curve)})")


def pc_psolve(srv, equip: IdxOrLabel, pc_curve: int) -> None:
    """
    Solve pressure-related PC curve values.

    Issues:
        GAP.PCPSOLV(equip, PcCurve)

    Parameters:
        srv: PetexServer.
        equip (IdxOrLabel): Equipment selector.
        pc_curve (int): Curve index per model definition.
    """
    srv.do_cmd(f"GAP.PCPSOLV({_wrap_idx_or_label(equip)}, {int(pc_curve)})")


# ============================================================
# Model-level Controls / Validation
# ============================================================

def mod_copy_controls(srv, model: IdxOrLabel, from_col: int, to_col: int, skip_fnas: bool = False) -> None:
    """
    Copy control settings from one column to another.

    Issues:
        GAP.MOD[i].CopyControls(from, to, SkipFNAs)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        from_col (int): Source control column.
        to_col (int): Target control column.
        skip_fnas (bool): If True, skip FNAs during copy.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].CopyControls({int(from_col)}, {int(to_col)}, {_b(skip_fnas)})"
    )


def mod_reset_controls(srv, model: IdxOrLabel, column: int) -> None:
    """
    Reset controls for a column in a model.

    Issues:
        GAP.MOD[i].ResetControls(column)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        column (int): Control column to reset.
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].ResetControls({int(column)})")


def mod_reset_schedule(srv, model: IdxOrLabel, sched_type: int, equip_type: int) -> None:
    """
    Reset schedule at model level.

    Issues:
        GAP.MOD[i].RESETSCHEDULE(type, equiptype)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        sched_type (int): Schedule type code.
        equip_type (int): Equipment type code.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].RESETSCHEDULE({int(sched_type)}, {int(equip_type)})"
    )


def mod_validate(srv, model: IdxOrLabel, solver_or_pred: int) -> None:
    """
    Validate a model.

    Issues:
        GAP.MOD[i].VALIDATE(solverorpred)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        solver_or_pred (int): 0=Solver validation, 1=Prediction validation.
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].VALIDATE({int(solver_or_pred)})")


# ============================================================
# Equipment / Node-level actions
# ============================================================

def equip_add_to_group(srv, model: IdxOrLabel, equip: IdxOrLabel, group: str) -> None:
    """
    Add an equipment to a named group.

    Issues:
        GAP.MOD[i].EQUIP[j].ADDTOGROUP("group")

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        group (str): Group name.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].ADDTOGROUP({_q(group)})"
    )


def equip_remove_from_group(srv, model: IdxOrLabel, equip: IdxOrLabel, group: str) -> None:
    """
    Remove an equipment from a group.

    Issues:
        GAP.MOD[i].EQUIP[j].REMOVEFROMGROUP("group")

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        group (str): Group name.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].REMOVEFROMGROUP({_q(group)})"
    )


def equip_remove_all_group_memberships(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Remove equipment from all groups.

    Issues:
        GAP.MOD[i].EQUIP[j].REMOVEALLGROUPMEMBERSHIPS()

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].REMOVEALLGROUPMEMBERSHIPS()"
    )


def equip_is_member_of_group(srv, model: IdxOrLabel, equip: IdxOrLabel, group: str) -> Optional[int]:
    """
    Test if equipment is part of a group.

    Issues:
        GAP.MOD[i].EQUIP[j].ISMEMBEROFGROUP("group")

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        group (str): Group name.

    Returns:
        Optional[int]: 1 if in group, 0 if not, None on failure.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].ISMEMBEROFGROUP({_q(group)})"
    )
    return _last_ret_int(srv)


def equip_enable(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Enable an equipment.

    Issues:
        GAP.MOD[i].EQUIP[j].ENABLE()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].ENABLE()")


def equip_disable(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Disable an equipment.

    Issues:
        GAP.MOD[i].EQUIP[j].DISABLE()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].DISABLE()")


def equip_bypass(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Bypass an equipment (ignore hydraulically).

    Issues:
        GAP.MOD[i].EQUIP[j].BYPASS()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].BYPASS()")


def equip_unbypass(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Un-bypass an equipment.

    Issues:
        GAP.MOD[i].EQUIP[j].UNBYPASS()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].UNBYPASS()")


def equip_mask(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Mask an equipment (remove from active calculation).

    Issues:
        GAP.MOD[i].EQUIP[j].MASK()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].MASK()")


def equip_unmask(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Unmask an equipment.

    Issues:
        GAP.MOD[i].EQUIP[j].UNMASK()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].UNMASK()")


def equip_is_connected_to(srv, model, equip_idx: int, other_idx: int) -> int | None:
    """
    Return 1 if EQUIP[equip_idx] is connected to EQUIP[other_idx], else 0.
    Uses full GAP path syntax.
    """
    cmd = (
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{equip_idx}]"
        f".ISCONNECTEDTO(GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{other_idx}])"
    )
    srv.do_cmd(cmd)
    return _last_ret_int(srv)


def equip_is_related_to(srv, model: IdxOrLabel, equip: IdxOrLabel, other: IdxOrLabel) -> Optional[int]:
    """
    Test if an equipment is related (not necessarily directly connected).

    Issues:
        GAP.MOD[i].EQUIP[j].ISRELATEDTO(other)

    Returns:
        Optional[int]: 1 if related, 0 if not, None if error.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].ISRELATEDTO({_wrap_idx_or_label(other)})"
    )
    return _last_ret_int(srv)


def equip_validate(srv, model: IdxOrLabel, equip: IdxOrLabel, solver_or_pred: int) -> None:
    """
    Validate an equipment.

    Issues:
        GAP.MOD[i].EQUIP[j].VALIDATE(solverorpred)
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].VALIDATE({int(solver_or_pred)})"
    )


# ============================================================
# Well-level convenience (DPControl, masking)
# ============================================================

def open_well(srv, model: IdxOrLabel, well: IdxOrLabel) -> None:
    """
    Open a well (set DPControl = CALCULATED, value=0).

    Equivalent to opening choke in UI.

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        well (IdxOrLabel): Well selector.
    """
    sel = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}]"
    srv.set_value(f"{sel}.DPControl", "CALCULATED")
    srv.set_value(f"{sel}.DPControlValue", 0)


def close_well(srv, model: IdxOrLabel, well: IdxOrLabel) -> None:
    """
    Close a well (set DPControl = FIXED=0).

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        well (IdxOrLabel): Well selector.
    """
    sel = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}]"
    srv.set_value(f"{sel}.DPControl", "FIXED")
    srv.set_value(f"{sel}.DPControlValue", 0)


def set_all_chokes_calculated(srv, model: IdxOrLabel) -> None:
    """
    Open all wells (set all DPControls to CALCULATED).

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
    """
    sel = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[$]"
    srv.set_value(f"{sel}.DPControl", "CALCULATED")
    srv.set_value(f"{sel}.DPControlValue", 0)


def mask_well(srv, model: IdxOrLabel, well: IdxOrLabel) -> None:
    """
    Mask a well (exclude from calculation).

    Issues:
        GAP.MOD[i].WELL[j].MASK()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].MASK()")


def unmask_well(srv, model: IdxOrLabel, well: IdxOrLabel) -> None:
    """
    Unmask a well.

    Issues:
        GAP.MOD[i].WELL[j].UNMASK()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].UNMASK()")


def set_gl_inj_depth_mode(srv, model: IdxOrLabel, well: IdxOrLabel, mode: int) -> None:
    """
    Set Gas Lift injection depth mode.

    Variable:
        MOD[i].WELL[j].GLInjDepthMode

    Parameters:
        mode (int): Depth mode code.
    """
    sel = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}]"
    srv.set_value(f"{sel}.GLInjDepthMode", int(mode))


# ============================================================
# IPR-level helpers
# ============================================================

def ipr_enable(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Enable a well IPR layer.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].ENABLE()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].ENABLE()"
    )


def ipr_disable(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Disable a well IPR layer.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].DISABLE()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].DISABLE()"
    )


def ipr_mask(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Mask an IPR layer.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].MASK()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].MASK()"
    )


def ipr_unmask(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Unmask an IPR layer.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].UNMASK()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].UNMASK()"
    )


def ipr_match(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Match IPR for a well layer.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].IPRMATCH()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].IPRMATCH()"
    )


def ipr_coning_match(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel) -> None:
    """
    Run coning match for IPR.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].CONINGMATCH()
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].CONINGMATCH()"
    )


def ipr_composition_import_prp_file(srv, model: IdxOrLabel, well: IdxOrLabel, layer: IdxOrLabel, filepath: str) -> None:
    """
    Import PRP file composition into IPR.

    Issues:
        GAP.MOD[i].EQUIP[j].IPR[k].COMPOSITION.IMPORTPRPFILE("FilePath")

    Parameters:
        filepath (str): Path to PRP file.
    """
    srv.do_cmd(
        "GAP.MOD[" + _wrap_idx_or_label(model) + "].EQUIP[" + _wrap_idx_or_label(well) + "]"
        + ".IPR[" + _wrap_idx_or_label(layer) + "].COMPOSITION.IMPORTPRPFILE(" + _q(filepath) + ")"
    )


# ============================================================
# Pipe / Tank helpers
# ============================================================

def pipe_do_match(srv, model: IdxOrLabel, pipe: IdxOrLabel) -> None:
    """
    Run DOMATCH on a pipe.

    Issues:
        GAP.MOD[i].PIPE[j].DOMATCH()
    """
    srv.do_cmd(f"GAP.MOD[{_wrap_idx_or_label(model)}].PIPE[{_wrap_idx_or_label(pipe)}].DOMATCH()")


def tank_calc_dc_cur_pres(srv, model: IdxOrLabel, tank: IdxOrLabel, cur_prod: float) -> None:
    """
    Calculate tank DC current pressure at given production.

    Issues:
        GAP.MOD[i].TANK[j].CalcDCTankCurPres(CurProd)

    Parameters:
        cur_prod (float): Current production value.
    """
    srv.do_cmd(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].TANK[{_wrap_idx_or_label(tank)}].CalcDCTankCurPres({float(cur_prod)})"
    )


# ============================================================
# Scheduling API (variables & commands)
# ============================================================

def schedule_count(srv, model: IdxOrLabel, equip: IdxOrLabel | None = None) -> Optional[int]:
    """
    Get the number of schedule rows.

    If `equip` is None → system-level count (MOD[i].SCHEDULE.COUNT),
    otherwise → equipment-level count (MOD[i].EQUIP[j].SCHEDULE.COUNT).

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel | None): Optional equipment selector.

    Returns:
        Optional[int]: Row count or None on failure.
    """
    if equip is None:
        tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].SCHEDULE.COUNT"
    else:
        tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].SCHEDULE.COUNT"
    try:
        val = srv.get_value(tag)
        return int(str(val)) if val is not None and str(val) != "" else None
    except Exception:
        return None


def simulation_schedule_count(srv, model: IdxOrLabel, equip_type: str, equip: IdxOrLabel) -> Optional[int]:
    """
    Get SIMULATIONSCHEDULE.COUNT for a specific equipment type/instance.

    Tag:
        MOD[i].<EquipType>[j].SIMULATIONSCHEDULE.COUNT

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip_type (str): E.g., 'Well', 'Joint', etc. (case-insensitive).
        equip (IdxOrLabel): Equipment selector.

    Returns:
        Optional[int]: Row count or None on failure.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].{equip_type}[{_wrap_idx_or_label(equip)}].SIMULATIONSCHEDULE.COUNT"
    try:
        val = srv.get_value(tag)
        return int(str(val)) if val is not None and str(val) != "" else None
    except Exception:
        return None


def set_schedule_row(
    srv,
    model: IdxOrLabel,
    equip: IdxOrLabel,
    row: int,
    *,
    date_str: str | None = None,
    event_type: str | None = None,
    lpar: str | None = None,
    cval: Any | None = None,
    lpar_index: int = 0,
    cval_index: int = 0,
) -> None:
    """
    Create or update a schedule row for an equipment.

    Tag base:
        MOD[i].EQUIP[j].SCHEDULE[row]

    Fields written (if provided):
        .DATE.DATESTR       ← date_str (e.g., "20/04/2012" with regional format)
        .TYPE               ← event keyword ("WELL_ON", "MASK", "CONSTRAINT_CHANGE", ...)
        .LPAR[lpar_index]   ← logical parameter (e.g., constraint name)
        .CVAL[cval_index]   ← value (numeric or string)

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        row (int): Zero-based row index.
        date_str (str | None): Date string.
        event_type (str | None): Event keyword.
        lpar (str | None): Event parameter name.
        cval (Any | None): Event parameter value.
        lpar_index (int): Index for LPAR[] array.
        cval_index (int): Index for CVAL[] array.
    """
    base = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].SCHEDULE[{int(row)}]"
    if date_str is not None:
        srv.set_value(f"{base}.DATE.DATESTR", date_str)
    if event_type is not None:
        srv.set_value(f"{base}.TYPE", event_type)
    if lpar is not None:
        srv.set_value(f"{base}.LPAR[{int(lpar_index)}]", lpar)
    if cval is not None:
        srv.set_value(f"{base}.CVAL[{int(cval_index)}]", cval)


def get_schedule_row(
    srv,
    model: IdxOrLabel,
    equip: IdxOrLabel,
    row: int,
    *,
    lpar_index: int = 0,
    cval_index: int = 0,
) -> dict[str, Any]:
    """
    Read a schedule row fields: date (numeric & string), type, LPAR[i], CVAL[i].

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        row (int): Row index.
        lpar_index (int): LPAR index to read.
        cval_index (int): CVAL index to read.

    Returns:
        dict[str, Any]: Keys: DATE_NUM, DATE_STR, TYPE, LPAR, CVAL.
    """
    base = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].SCHEDULE[{int(row)}]"
    out: dict[str, Any] = {}
    out["DATE_NUM"] = srv.get_value(f"{base}.DATE")
    out["DATE_STR"] = srv.get_value(f"{base}.DATE.DATESTR")
    out["TYPE"] = srv.get_value(f"{base}.TYPE")
    out["LPAR"] = srv.get_value(f"{base}.LPAR[{int(lpar_index)}]")
    out["CVAL"] = srv.get_value(f"{base}.CVAL[{int(cval_index)}]")
    return out


def schedule_reset_system(srv, model: IdxOrLabel) -> None:
    """
    Reset system-level schedule (clear rows).

    Writes:
        MOD[i].SCHEDULE.RESET = 1
    """
    srv.set_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].SCHEDULE.RESET", 1)


def schedule_reset_equip(srv, model: IdxOrLabel, equip: IdxOrLabel) -> None:
    """
    Reset equipment-level schedule (clear rows).

    Writes:
        MOD[i].EQUIP[j].SCHEDULE.RESET = 1
    """
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].SCHEDULE.RESET",
        1,
    )


def apply_schedule_to(srv, date_str: str) -> None:
    """
    Apply the schedule up to a given date string.

    Issues:
        GAP.APPLYSCHEDULETO({DD/MM/YYYY})

    Note: GAP expects the date inside braces (handled by the call).
    """
    srv.do_cmd(f"GAP.APPLYSCHEDULETO({{{date_str}}})")


def clear_applied_schedule(srv) -> None:
    """
    Clear previously applied schedule date.

    Issues:
        GAP.CLEARAPPLIEDSCHEDULE()
    """
    srv.do_cmd("GAP.CLEARAPPLIEDSCHEDULE()")


# ============================================================
# Constraints API (system, node, abandonment)
# ============================================================

def set_system_constraint(srv, model: IdxOrLabel, name: str, value: Any) -> None:
    """
    Set a model-level (system) constraint.

    Tag:
        MOD[i].<NAME>

    Examples:
        "MAXQGAS", "MAXQLIQ", "MAXQOIL", "MAXQWAT", "MAXQTOTGAS", ...
    """
    srv.set_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{name}", value)


def get_system_constraint(srv, model: IdxOrLabel, name: str) -> Any:
    """
    Get a model-level (system) constraint value.

    Parameters:
        name (str): Constraint name (e.g., "MAXQGAS").

    Returns:
        Any: Value as returned by DoGet.
    """
    return srv.get_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{name}")


def set_node_constraint(
    srv,
    model: IdxOrLabel,
    node_type: str,
    node: IdxOrLabel,
    name: str,
    value: Any,
) -> None:
    """
    Set a node-level constraint value.

    Tag:
        MOD[i].<NODETYPE>[j].<NAME>

    Parameters:
        node_type (str): 'WELL', 'PIPE', 'COMPRESSOR', 'SEPARATOR', etc.
        node (IdxOrLabel): Node selector.
        name (str): Constraint name.
        value (Any): Value to write.
    """
    node_type = node_type.upper()
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{_wrap_idx_or_label(node)}].{name}",
        value,
    )


def get_node_constraint(
    srv,
    model: IdxOrLabel,
    node_type: str,
    node: IdxOrLabel,
    name: str,
) -> Any:
    """
    Get a node-level constraint value.

    Returns:
        Any: Value as returned by DoGet.
    """
    node_type = node_type.upper()
    return srv.get_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{_wrap_idx_or_label(node)}].{name}"
    )


def set_abandonment_constraint(
    srv,
    model: IdxOrLabel,
    well: IdxOrLabel,
    layer: IdxOrLabel,
    name: str,
    value: Any,
) -> None:
    """
    Set a well-layer abandonment constraint at IPR level.

    Tag:
        MOD[i].WELL[j].IPR[k].<NAME>

    Examples:
        ABMAXGOR, ABMAXWC, etc.
    """
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].{name}",
        value,
    )


def get_abandonment_constraint(
    srv,
    model: IdxOrLabel,
    well: IdxOrLabel,
    layer: IdxOrLabel,
    name: str,
) -> Any:
    """
    Get a well-layer abandonment constraint value (IPR-level).

    Returns:
        Any: Value as returned by DoGet.
    """
    return srv.get_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].IPR[{_wrap_idx_or_label(layer)}].{name}"
    )


# -------- System-level (MOD) convenience wrappers --------

def get_max_qgas_system(srv, model: IdxOrLabel):
    """
    Get system MAXQGAS.
    """
    return get_system_constraint(srv, model, "MAXQGAS")


def set_max_qgas_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXQGAS.

    Parameters:
        value: Numeric (units as per model).
    """
    set_system_constraint(srv, model, "MAXQGAS", value)


def get_max_qliq_system(srv, model: IdxOrLabel):
    """
    Get system MAXQLIQ.
    """
    return get_system_constraint(srv, model, "MAXQLIQ")


def set_max_qliq_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXQLIQ.
    """
    set_system_constraint(srv, model, "MAXQLIQ", value)


def get_max_qoil_system(srv, model: IdxOrLabel):
    """
    Get system MAXQOIL.
    """
    return get_system_constraint(srv, model, "MAXQOIL")


def set_max_qoil_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXQOIL.
    """
    set_system_constraint(srv, model, "MAXQOIL", value)


def get_max_qwat_system(srv, model: IdxOrLabel):
    """
    Get system MAXQWAT.
    """
    return get_system_constraint(srv, model, "MAXQWAT")


def set_max_qwat_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXQWAT.
    """
    set_system_constraint(srv, model, "MAXQWAT", value)


def get_max_qtotgas_system(srv, model: IdxOrLabel):
    """
    Get system MAXQTOTGAS.
    """
    return get_system_constraint(srv, model, "MAXQTOTGAS")


def set_max_qtotgas_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXQTOTGAS.
    """
    set_system_constraint(srv, model, "MAXQTOTGAS", value)


def set_max_gross_heating_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXGROSSHEATING.
    """
    set_system_constraint(srv, model, "MAXGROSSHEATING", value)


def set_max_spec_gross_heating_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXSPECGROSSHEATING.
    """
    set_system_constraint(srv, model, "MAXSPECGROSSHEATING", value)


def set_max_pco2_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXPCO2.
    """
    set_system_constraint(srv, model, "MAXPCO2", value)


def set_max_ph2s_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXPH2S.
    """
    set_system_constraint(srv, model, "MAXPH2S", value)


def set_max_pn2_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXPN2.
    """
    set_system_constraint(srv, model, "MAXPN2", value)


def set_max_sog_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXSOG.
    """
    set_system_constraint(srv, model, "MAXSOG", value)


def set_max_pow_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXPOW.
    """
    set_system_constraint(srv, model, "MAXPOW", value)


def set_max_power_fluid_system(srv, model: IdxOrLabel, value):
    """
    Set system MAXPOWERFLUID.
    """
    set_system_constraint(srv, model, "MAXPOWERFLUID", value)


# -------- Well-level convenience wrappers --------

def set_max_qgas_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MAXQGAS.
    """
    set_node_constraint(srv, model, "WELL", well, "MAXQGAS", value)


def get_max_qgas_well(srv, model: IdxOrLabel, well: IdxOrLabel):
    """
    Get WELL.MAXQGAS.
    """
    return get_node_constraint(srv, model, "WELL", well, "MAXQGAS")


def set_min_qgas_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MINQGAS.
    """
    set_node_constraint(srv, model, "WELL", well, "MINQGAS", value)


def set_max_qliq_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MAXQLIQ.
    """
    set_node_constraint(srv, model, "WELL", well, "MAXQLIQ", value)


def set_min_qliq_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MINQLIQ.
    """
    set_node_constraint(srv, model, "WELL", well, "MINQLIQ", value)


def set_max_qoil_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MAXQOIL.
    """
    set_node_constraint(srv, model, "WELL", well, "MAXQOIL", value)


def set_max_qwat_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MAXQWAT.
    """
    set_node_constraint(srv, model, "WELL", well, "MAXQWAT", value)


def set_max_pwf_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MAXPWF.
    """
    set_node_constraint(srv, model, "WELL", well, "MAXPWF", value)


def get_max_pwf_well(srv, model: IdxOrLabel, well: IdxOrLabel):
    """
    Get WELL.MAXPWF.
    """
    return get_node_constraint(srv, model, "WELL", well, "MAXPWF")


def set_min_pwf_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.MINPWF.
    """
    set_node_constraint(srv, model, "WELL", well, "MINPWF", value)


def set_ginj_limits_well(srv, model: IdxOrLabel, well: IdxOrLabel, *, max_ginj=None, min_ginj=None, min_ginj_nc=None) -> None:
    """
    Convenience to set any of: MAXGINJ, MINGINJ, MINGINJNC.

    Parameters:
        max_ginj, min_ginj, min_ginj_nc: Values or None to skip.
    """
    if max_ginj is not None:
        set_node_constraint(srv, model, "WELL", well, "MAXGINJ", max_ginj)
    if min_ginj is not None:
        set_node_constraint(srv, model, "WELL", well, "MINGINJ", min_ginj)
    if min_ginj_nc is not None:
        set_node_constraint(srv, model, "WELL", well, "MINGINJNC", min_ginj_nc)


def set_shutin_priority_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.SHUTINPRI.
    """
    set_node_constraint(srv, model, "WELL", well, "SHUTINPRI", value)


def set_opt_weight_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.OPTWEIGTH (optimizer weight).
    """
    set_node_constraint(srv, model, "WELL", well, "OPTWEIGTH", value)


# Lift-specific (ESP/PCP/ALQ) at wells

def set_esp_freq_max_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.ESPFreqMax.
    """
    set_node_constraint(srv, model, "WELL", well, "ESPFreqMax", value)


def set_esp_freq_min_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.ESPFreqMin.
    """
    set_node_constraint(srv, model, "WELL", well, "ESPFreqMin", value)


def set_pcp_speed_max_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.PCPSpeedMax.
    """
    set_node_constraint(srv, model, "WELL", well, "PCPSpeedMax", value)


def set_pcp_speed_min_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.PCPSpeedMin.
    """
    set_node_constraint(srv, model, "WELL", well, "PCPSpeedMin", value)


def set_alq_value_max_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.AlqValueMax.
    """
    set_node_constraint(srv, model, "WELL", well, "AlqValueMax", value)


def set_alq_value_min_well(srv, model: IdxOrLabel, well: IdxOrLabel, value):
    """
    Set WELL.AlqValueMin.
    """
    set_node_constraint(srv, model, "WELL", well, "AlqValueMin", value)


# -------- Pipe-level convenience --------

def set_max_pressure_pipe(srv, model: IdxOrLabel, pipe: IdxOrLabel, value):
    """
    Set PIPE.MAXPRESSURE.
    """
    set_node_constraint(srv, model, "PIPE", pipe, "MAXPRESSURE", value)


def set_max_velocity_pipe(srv, model: IdxOrLabel, pipe: IdxOrLabel, value):
    """
    Set PIPE.MAXVELOCITY.
    """
    set_node_constraint(srv, model, "PIPE", pipe, "MAXVELOCITY", value)


def set_max_cfactor_pipe(srv, model: IdxOrLabel, pipe: IdxOrLabel, value):
    """
    Set PIPE.MAXCFACTOR.
    """
    set_node_constraint(srv, model, "PIPE", pipe, "MAXCFACTOR", value)


# -------- Compressor-level convenience --------

def set_max_pow_compressor(srv, model: IdxOrLabel, comp: IdxOrLabel, value) -> None:
    """
    Set COMPRESSOR.MAXPOW.
    """
    set_node_constraint(srv, model, "COMPRESSOR", comp, "MAXPOW", value)


def get_max_pow_compressor(srv, model: IdxOrLabel, comp: IdxOrLabel):
    """
    Get COMPRESSOR.MAXPOW.
    """
    return get_node_constraint(srv, model, "COMPRESSOR", comp, "MAXPOW")


def set_max_sog_compressor(srv, model: IdxOrLabel, comp: IdxOrLabel, value) -> None:
    """
    Set COMPRESSOR.MAXSOG.
    """
    set_node_constraint(srv, model, "COMPRESSOR", comp, "MAXSOG", value)


def get_max_sog_compressor(srv, model: IdxOrLabel, comp: IdxOrLabel):
    """
    Get COMPRESSOR.MAXSOG.
    """
    return get_node_constraint(srv, model, "COMPRESSOR", comp, "MAXSOG")


def set_max_pressure_compressor(srv, model: IdxOrLabel, comp: IdxOrLabel, value) -> None:
    """
    Set COMPRESSOR.MAXPRESSURE.
    """
    set_node_constraint(srv, model, "COMPRESSOR", comp, "MAXPRESSURE", value)


# -------- Pump-level convenience --------

def set_max_power_fluid_pump(srv, model: IdxOrLabel, pump: IdxOrLabel, value) -> None:
    """
    Set PUMP.MAXPOWERFLUID.
    """
    set_node_constraint(srv, model, "PUMP", pump, "MAXPOWERFLUID", value)


def get_max_power_fluid_pump(srv, model: IdxOrLabel, pump: IdxOrLabel):
    """
    Get PUMP.MAXPOWERFLUID.
    """
    return get_node_constraint(srv, model, "PUMP", pump, "MAXPOWERFLUID")


def set_max_pressure_pump(srv, model: IdxOrLabel, pump: IdxOrLabel, value) -> None:
    """
    Set PUMP.MAXPRESSURE.
    """
    set_node_constraint(srv, model, "PUMP", pump, "MAXPRESSURE", value)


def set_max_velocity_pump(srv, model: IdxOrLabel, pump: IdxOrLabel, value) -> None:
    """
    Set PUMP.MAXVELOCITY.
    """
    set_node_constraint(srv, model, "PUMP", pump, "MAXVELOCITY", value)


# -------- Separator-level convenience --------

def set_max_pressure_separator(srv, model: IdxOrLabel, sep: IdxOrLabel, value) -> None:
    """
    Set SEPARATOR.MAXPRESSURE.
    """
    set_node_constraint(srv, model, "SEPARATOR", sep, "MAXPRESSURE", value)


def set_max_qgas_separator(srv, model: IdxOrLabel, sep: IdxOrLabel, value) -> None:
    """
    Set SEPARATOR.MAXQGAS.
    """
    set_node_constraint(srv, model, "SEPARATOR", sep, "MAXQGAS", value)


def set_max_qliq_separator(srv, model: IdxOrLabel, sep: IdxOrLabel, value) -> None:
    """
    Set SEPARATOR.MAXQLIQ.
    """
    set_node_constraint(srv, model, "SEPARATOR", sep, "MAXQLIQ", value)


# -------- Binding & Potential flags --------

def set_system_constraint_binding(srv, model: IdxOrLabel, name: str, binding: bool) -> None:
    """
    Set the BINDING flag for a system constraint.

    Writes:
        MOD[i].<NAME>BINDING = 0/1
    """
    srv.set_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{name}BINDING", 1 if binding else 0)


def get_system_constraint_binding(srv, model: IdxOrLabel, name: str) -> Optional[int]:
    """
    Get the BINDING flag value for a system constraint.

    Returns:
        Optional[int]: 0/1 or None on failure.
    """
    try:
        v = srv.get_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{name}BINDING")
        return int(str(v)) if v is not None and str(v) != "" else None
    except Exception:
        return None


def set_node_constraint_binding(srv, model: IdxOrLabel, node_type: str, node: IdxOrLabel, name: str, binding: bool) -> None:
    """
    Set the BINDING flag at node level.

    Writes:
        MOD[i].<NODETYPE>[j].<NAME>BINDING = 0/1
    """
    node_type = node_type.upper()
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{_wrap_idx_or_label(node)}].{name}BINDING",
        1 if binding else 0,
    )


def get_node_constraint_binding(srv, model: IdxOrLabel, node_type: str, node: IdxOrLabel, name: str) -> Optional[int]:
    """
    Get the BINDING flag at node level.

    Returns:
        Optional[int]: 0/1 or None on failure.
    """
    node_type = node_type.upper()
    try:
        v = srv.get_value(
            f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{_wrap_idx_or_label(node)}].{name}BINDING"
        )
        return int(str(v)) if v is not None and str(v) != "" else None
    except Exception:
        return None


def set_system_constraint_potential(srv, model: IdxOrLabel, name: str, keep_active: bool) -> None:
    """
    Set the POTENTIAL flag for a system constraint.

    Writes:
        MOD[i].<NAME>POTENTIAL = 0/1
    """
    srv.set_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{name}POTENTIAL", 1 if keep_active else 0)


def set_node_constraint_potential(srv, model: IdxOrLabel, node_type: str, node: IdxOrLabel, name: str, keep_active: bool) -> None:
    """
    Set the POTENTIAL flag at node level.

    Writes:
        MOD[i].<NODETYPE>[j].<NAME>POTENTIAL = 0/1
    """
    node_type = node_type.upper()
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{_wrap_idx_or_label(node)}].{name}POTENTIAL",
        1 if keep_active else 0,
    )

# ============================================================
# Generic variable utilities (power users)
# ============================================================

def get_model_var(srv, model: IdxOrLabel, var: str) -> Any:
    """
    Get a variable directly at the model level.

    Example:
        get_model_var(srv, "PROD", "MAXQGAS")

    Returns:
        Any: Value returned by DoGet.
    """
    return srv.get_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{var}")


def set_model_var(srv, model: IdxOrLabel, var: str, value: Any) -> None:
    """
    Set a variable directly at the model level.

    Example:
        set_model_var(srv, "PROD", "MAXQGAS", 1e6)
    """
    srv.set_value(f"GAP.MOD[{_wrap_idx_or_label(model)}].{var}", value)


def get_equip_var(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str) -> Any:
    """
    Get an equipment-level variable.

    Tag:
        MOD[i].EQUIP[j].<var>
    """
    return srv.get_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}"
    )


def set_equip_var(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str, value: Any) -> None:
    """
    Set an equipment-level variable.

    Tag:
        MOD[i].EQUIP[j].<var>
    """
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}",
        value,
    )


def get_well_var(srv, model: IdxOrLabel, well: IdxOrLabel, var: str) -> Any:
    """
    Get a well-level variable.

    Tag:
        MOD[i].WELL[j].<var>
    """
    return srv.get_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].{var}"
    )


def set_well_var(srv, model: IdxOrLabel, well: IdxOrLabel, var: str, value: Any) -> None:
    """
    Set a well-level variable.

    Tag:
        MOD[i].WELL[j].<var>
    """
    srv.set_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].{var}",
        value,
    )

# ============================================================
# Generic Array / Matrix variable helpers
# ============================================================

def get_equip_array(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str, i: int):
    """
    Get a single element from an equipment array variable.

    Tag pattern:
        MOD[i].EQUIP[j].<var>[i]

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        var (str): Variable name (e.g., "PumpRateConstraint").
        i (int): Zero-based array index.

    Returns:
        Any: Value returned by DoGet.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}[{int(i)}]"
    return srv.get_value(tag)


def set_equip_array(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str, i: int, value) -> None:
    """
    Set a single element in an equipment array variable.

    Tag pattern:
        MOD[i].EQUIP[j].<var>[i]

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        var (str): Variable name.
        i (int): Zero-based index.
        value: Value to assign.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}[{int(i)}]"
    srv.set_value(tag, value)


def get_equip_matrix(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str, i: int, j: int):
    """
    Get a single element from an equipment matrix variable.

    Tag pattern:
        MOD[i].EQUIP[j].<var>[i][j]

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        var (str): Variable name.
        i (int): Row index.
        j (int): Column index.

    Returns:
        Any: Value returned by DoGet.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}[{int(i)}][{int(j)}]"
    return srv.get_value(tag)


def set_equip_matrix(srv, model: IdxOrLabel, equip: IdxOrLabel, var: str, i: int, j: int, value) -> None:
    """
    Set a single element in an equipment matrix variable.

    Tag pattern:
        MOD[i].EQUIP[j].<var>[i][j]

    Parameters:
        srv: PetexServer.
        model (IdxOrLabel): Model selector.
        equip (IdxOrLabel): Equipment selector.
        var (str): Variable name.
        i (int): Row index.
        j (int): Column index.
        value: Value to assign.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var}[{int(i)}][{int(j)}]"
    srv.set_value(tag, value)


# ============================================================
# Pattern-based bulk enable/disable/mask via wildcard on labels
# ============================================================

_NODE_TYPES_SCAN = [
    "WELL", "PIPE", "JOINT", "COMPRESSOR", "PUMP", "SEPARATOR", "TANK", "VALVE", "MANIFOLD"
]

def _node_count(srv, model: IdxOrLabel, node_type: str) -> int:
    """
    Return number of nodes for a given type.

    Tag:
        MOD[i].<NODETYPE>.COUNT
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}.COUNT"
    v = srv.get_value(tag)
    try:
        return int(str(v))
    except Exception:
        return 0


def _node_label(srv, model: IdxOrLabel, node_type: str, idx: int) -> str:
    """
    Get the label of a node by index.

    Tag:
        MOD[i].<NODETYPE>[idx].LABEL
    """
    return str(srv.get_value(
        f"GAP.MOD[{_wrap_idx_or_label(model)}].{node_type}[{int(idx)}].LABEL"
    ))


def enable_by_pattern(srv, model: IdxOrLabel, pattern: str, node_types: list[str] | None = None) -> list[str]:
    """
    Enable all equipments whose LABEL matches the given pattern.

    Parameters:
        pattern (str): Shell-style wildcard (e.g., "W*", "SEP_*").
        node_types (list[str] | None): Types to scan (defaults to WELL, PIPE, etc.).

    Returns:
        list[str]: Labels of nodes changed.
    """
    changed: list[str] = []
    nts = node_types or _NODE_TYPES_SCAN
    for nt in nts:
        n = _node_count(srv, model, nt)
        for i in range(n):
            label = _node_label(srv, model, nt, i)
            if fnmatch(label, pattern):
                equip_enable(srv, model, label)
                changed.append(label)
    return changed


def disable_by_pattern(srv, model: IdxOrLabel, pattern: str, node_types: list[str] | None = None) -> list[str]:
    """
    Disable all equipments matching a label pattern.

    Returns:
        list[str]: Labels changed.
    """
    changed: list[str] = []
    nts = node_types or _NODE_TYPES_SCAN
    for nt in nts:
        n = _node_count(srv, model, nt)
        for i in range(n):
            label = _node_label(srv, model, nt, i)
            if fnmatch(label, pattern):
                equip_disable(srv, model, label)
                changed.append(label)
    return changed


def mask_by_pattern(srv, model: IdxOrLabel, pattern: str, node_types: list[str] | None = None) -> list[str]:
    """
    Mask all equipments matching a label pattern.

    Returns:
        list[str]: Labels changed.
    """
    changed: list[str] = []
    nts = node_types or _NODE_TYPES_SCAN
    for nt in nts:
        n = _node_count(srv, model, nt)
        for i in range(n):
            label = _node_label(srv, model, nt, i)
            if fnmatch(label, pattern):
                equip_mask(srv, model, label)
                changed.append(label)
    return changed


def unmask_by_pattern(srv, model: IdxOrLabel, pattern: str, node_types: list[str] | None = None) -> list[str]:
    """
    Unmask all equipments matching a label pattern.

    Returns:
        list[str]: Labels changed.
    """
    changed: list[str] = []
    nts = node_types or _NODE_TYPES_SCAN
    for nt in nts:
        n = _node_count(srv, model, nt)
        for i in range(n):
            label = _node_label(srv, model, nt, i)
            if fnmatch(label, pattern):
                equip_unmask(srv, model, label)
                changed.append(label)
    return changed


# ============================================================
# Constraint presets (apply multiple constraints in one call)
# ============================================================

def _parse_constraint_value(entry: Any) -> tuple[Any, Optional[bool], Optional[bool]]:
    """
    Parse a constraint entry into (value, binding, potential).

    Supports:
        - dict {"value": v, "binding": b, "potential": p}
        - tuple/list [v, b, p]
        - scalar (value only)
    """
    if isinstance(entry, dict):
        return entry.get("value"), entry.get("binding"), entry.get("potential")
    if isinstance(entry, (list, tuple)):
        v = entry[0] if len(entry) > 0 else None
        b = entry[1] if len(entry) > 1 else None
        p = entry[2] if len(entry) > 2 else None
        return v, b, p
    return entry, None, None


def apply_constraints_system(
    srv,
    model: IdxOrLabel,
    constraints: dict[str, Any],
    *,
    default_binding: Optional[bool] = None,
    default_potential: Optional[bool] = None,
) -> None:
    """
    Apply multiple constraints at MOD level in one call.

    Parameters:
        constraints (dict): e.g.,
            {
                "MAXQGAS": 5e6,
                "MAXQLIQ": {"value": 10000, "binding": True, "potential": False},
                "MAXPWF": (120e5, True, None),
            }
        default_binding (bool | None): Default if not provided.
        default_potential (bool | None): Default if not provided.
    """
    for name, entry in constraints.items():
        value, b, p = _parse_constraint_value(entry)
        set_system_constraint(srv, model, name, value)
        if b is not None or default_binding is not None:
            set_system_constraint_binding(srv, model, name, b if b is not None else default_binding)
        if p is not None or default_potential is not None:
            set_system_constraint_potential(srv, model, name, p if p is not None else default_potential)


def apply_constraints_node(
    srv,
    model: IdxOrLabel,
    node_type: str,
    node: IdxOrLabel,
    constraints: dict[str, Any],
    *,
    default_binding: Optional[bool] = None,
    default_potential: Optional[bool] = None,
) -> None:
    """
    Apply multiple constraints at node level in one call.

    Example:
        apply_constraints_node(srv, "PROD", "WELL", "W1",
            {"MAXQGAS": (1e6, True, None), "MAXPWF": 150e5})
    """
    node_type = node_type.upper()
    for name, entry in constraints.items():
        value, b, p = _parse_constraint_value(entry)
        set_node_constraint(srv, model, node_type, node, name, value)
        if b is not None or default_binding is not None:
            set_node_constraint_binding(
                srv, model, node_type, node, name, b if b is not None else default_binding
            )
        if p is not None or default_potential is not None:
            set_node_constraint_potential(
                srv, model, node_type, node, name, p if p is not None else default_potential
            )


# ============================================================
# Pump / ESP / PCP Curve Utilities (generic series helpers)
# ============================================================

def curve_count_equip(srv, model: IdxOrLabel, equip: IdxOrLabel, var_path: str) -> Optional[int]:
    """
    Get number of points in an equipment-level curve series.

    Tag:
        MOD[i].EQUIP[j].<var_path>.COUNT
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].EQUIP[{_wrap_idx_or_label(equip)}].{var_path}.COUNT"
    try:
        v = srv.get_value(tag)
        return int(str(v)) if v is not None and str(v) != "" else None
    except Exception:
        return None


def curve_get_point_equip(srv, model: IdxOrLabel, equip: IdxOrLabel, var_path: str, i: int):
    """
    Get a point from an equipment-level curve series.

    Tag:
        MOD[i].EQUIP[j].<var_path>[i]
    """
    return get_equip_array(srv, model, equip, var_path, i)


def curve_set_point_equip(srv, model: IdxOrLabel, equip: IdxOrLabel, var_path: str, i: int, value) -> None:
    """
    Set a point in an equipment-level curve series.
    """
    set_equip_array(srv, model, equip, var_path, i, value)


def curve_count_well(srv, model: IdxOrLabel, well: IdxOrLabel, var_path: str) -> Optional[int]:
    """
    Get number of points in a well-level curve series.

    Tag:
        MOD[i].WELL[j].<var_path>.COUNT
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].{var_path}.COUNT"
    try:
        v = srv.get_value(tag)
        return int(str(v)) if v is not None and str(v) != "" else None
    except Exception:
        return None


def curve_get_point_well(srv, model: IdxOrLabel, well: IdxOrLabel, var_path: str, i: int):
    """
    Get a point from a well-level curve series.

    Tag:
        MOD[i].WELL[j].<var_path>[i]
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].{var_path}[{int(i)}]"
    return srv.get_value(tag)


def curve_set_point_well(srv, model: IdxOrLabel, well: IdxOrLabel, var_path: str, i: int, value) -> None:
    """
    Set a point in a well-level curve series.
    """
    tag = f"GAP.MOD[{_wrap_idx_or_label(model)}].WELL[{_wrap_idx_or_label(well)}].{var_path}[{int(i)}]"
    srv.set_value(tag, value)


def curve_bulk_set_well(
    srv,
    model: IdxOrLabel,
    well: IdxOrLabel,
    var_path: str,
    values: Iterable[Any],
    start_index: int = 0,
) -> int:
    """
    Bulk write a sequence of values into a well-level curve series.

    Parameters:
        values (Iterable): Sequence of values.
        start_index (int): Starting array index.

    Returns:
        int: Last index written.
    """
    i = start_index
    for val in values:
        curve_set_point_well(srv, model, well, var_path, i, val)
        i += 1
    return i - 1


# ============================================================
# Bulk Schedule Generators & Utilities
# ============================================================

_VALID_EVENT_TYPES = {
    "WELL_ON", "WELL_OFF", "MASK", "UNMASK", "CONSTRAINT_CHANGE",
}

def next_schedule_row(srv, model: IdxOrLabel, equip: IdxOrLabel) -> int:
    """
    Get next appendable row index for EQUIP schedule.

    Uses:
        schedule_count + 1
    """
    c = schedule_count(srv, model, equip) or 0
    return int(c)


def schedule_append_event(
    srv,
    model: IdxOrLabel,
    equip: IdxOrLabel,
    date_str: str,
    event_type: str,
    *,
    lpar: str | None = None,
    cval: Any | None = None,
    lpar_index: int = 0,
    cval_index: int = 0,
) -> int:
    """
    Append a schedule event row.

    Returns:
        int: Row index used.
    """
    et = event_type.upper()
    if et not in _VALID_EVENT_TYPES:
        raise ValueError(f"Unsupported schedule event_type: {event_type}")
    row = next_schedule_row(srv, model, equip)
    set_schedule_row(
        srv,
        model,
        equip,
        row,
        date_str=date_str,
        event_type=et,
        lpar=lpar,
        cval=cval,
        lpar_index=lpar_index,
        cval_index=cval_index,
    )
    return row


def schedule_append_well_on(srv, model: IdxOrLabel, well: IdxOrLabel, date_str: str) -> int:
    """Append WELL_ON event for a well."""
    return schedule_append_event(srv, model, well, date_str, "WELL_ON")


def schedule_append_well_off(srv, model: IdxOrLabel, well: IdxOrLabel, date_str: str) -> int:
    """Append WELL_OFF event for a well."""
    return schedule_append_event(srv, model, well, date_str, "WELL_OFF")


def schedule_append_mask(srv, model: IdxOrLabel, equip: IdxOrLabel, date_str: str) -> int:
    """Append MASK event for an equipment."""
    return schedule_append_event(srv, model, equip, date_str, "MASK")


def schedule_append_unmask(srv, model: IdxOrLabel, equip: IdxOrLabel, date_str: str) -> int:
    """Append UNMASK event for an equipment."""
    return schedule_append_event(srv, model, equip, date_str, "UNMASK")


def schedule_append_constraint_change(
    srv,
    model: IdxOrLabel,
    equip: IdxOrLabel,
    date_str: str,
    name: str,
    value: Any,
    *,
    lpar_index: int = 0,
    cval_index: int = 0,
) -> int:
    """Append CONSTRAINT_CHANGE event for a node."""
    return schedule_append_event(
        srv,
        model,
        equip,
        date_str,
        "CONSTRAINT_CHANGE",
        lpar=name,
        cval=value,
        lpar_index=lpar_index,
        cval_index=cval_index,
    )


def schedule_wells_on_off_by_dates(
    srv,
    model: IdxOrLabel,
    wells: Iterable[IdxOrLabel],
    *,
    on_dates: Iterable[str] = (),
    off_dates: Iterable[str] = (),
) -> dict[str, list[int]]:
    """
    Append WELL_ON/OFF events for each well on given dates.

    Returns:
        dict[str, list[int]]: {well_label: [row_indices]}
    """
    result: dict[str, list[int]] = {}
    for w in wells:
        label = str(w)
        rows: list[int] = []
        for d in on_dates:
            rows.append(schedule_append_well_on(srv, model, w, d))
        for d in off_dates:
            rows.append(schedule_append_well_off(srv, model, w, d))
        result[label] = rows
    return result


def schedule_apply_constraints_for_nodes(
    srv,
    model: IdxOrLabel,
    nodes: Iterable[tuple[str, IdxOrLabel]],
    date_str: str,
    constraints: dict[str, Any],
) -> list[tuple[str, str, int]]:
    """
    Apply multiple constraints for a list of nodes at a given date.

    Parameters:
        nodes (list[tuple]): List of (node_type, node).
        constraints (dict): Constraints dict.

    Returns:
        list[tuple[str, str, int]]: (node_type, node_label, row_index).
    """
    out: list[tuple[str, str, int]] = []
    for node_type, node in nodes:
        for name, entry in constraints.items():
            value, _, _ = _parse_constraint_value(entry)
            row = schedule_append_constraint_change(srv, model, node, date_str, name, value)
            out.append((node_type.upper(), str(node), row))
    return out


def schedule_for_wells_by_pattern(
    srv,
    model: IdxOrLabel,
    pattern: str,
    *,
    on_dates: Iterable[str] = (),
    off_dates: Iterable[str] = (),
) -> dict[str, list[int]]:
    """
    Find wells by label pattern and schedule WELL_ON/OFF events.

    Returns:
        dict[str, list[int]]
    """
    wells = []
    n_wells = _node_count(srv, model, "WELL")
    for i in range(n_wells):
        label = _node_label(srv, model, "WELL", i)
        if fnmatch(label, pattern):
            wells.append(label)
    return schedule_wells_on_off_by_dates(srv, model, wells, on_dates=on_dates, off_dates=off_dates)


# ============================================================
# Final convenience helpers
# ============================================================

def get_all(srv, tag: str) -> List[str]:
    """
    Return a list of all values for a GAP tag that supports [$].

    Example:
        get_all(srv, "GAP.MOD[0].EQUIP[$].Label")
    """
    return split_gap_list(srv.get_value(tag))


def get_all_equips(srv):
    """
    Return a dictionary {equipment_type: [labels]} for all equipments.

    Uses:
        MOD[0].EQUIP[$].Type
        MOD[0].EQUIP[$].Label

    Skips blank entries.
    """
    types = get_all(srv, "GAP.MOD[0].EQUIP[$].Type")
    labels = get_all(srv, "GAP.MOD[0].EQUIP[$].Label")

    equips = {}
    for t, l in zip(types, labels):
        if not t or not str(t).strip() or not l or not str(l).strip():
            continue
        equips.setdefault(t, []).append(l)

    return equips

# --- Equipment Helpers ---
def get_equip_count(srv, model_name: str, etype: str) -> int:
    """
    Return number of equipments of a given type (PIPE, SEP, WELL, etc.).

    Uses:
        GAP.MOD[{model_name}].{etype}.COUNT
    """
    return int(srv.get_value(f"GAP.MOD[{model_name}].{etype}.COUNT") or 0)


def get_equip_uid(srv, model_name: str, etype: str, ref: int | str) -> str | None:
    """
    Return UniqueID of equipment by index, label, or UID.

    Parameters:
        ref (int | str): index (0-based), label, or UniqueID.

    Uses:
        GAP.MOD[{model_name}].{etype}[{ref}].UniqueID
    """
    return srv.get_value(f"GAP.MOD[{model_name}].{etype}[{_wrap_idx_or_label(ref)}].UniqueID")


def get_pipe_endpoints(srv, model_name: str, etype: str, ref: int | str):
    """
    Return (EndA, EndB) UniqueIDs of a pipe-like equipment.

    Parameters:
        ref (int | str): index (0-based), label, or UniqueID.

    Uses:
        GAP.MOD[{model_name}].{etype}[{ref}].ENDA
        GAP.MOD[{model_name}].{etype}[{ref}].ENDB
    """
    enda = srv.get_value(f"GAP.MOD[{model_name}].{etype}[{_wrap_idx_or_label(ref)}].ENDA")
    endb = srv.get_value(f"GAP.MOD[{model_name}].{etype}[{_wrap_idx_or_label(ref)}].ENDB")
    return enda, endb

# --- Mask / Unmask ---
def mask(srv, ref: int | str, model_name="PROD"):
    """
    Mask (close) a pipe or equipment by index, label, or UniqueID.

    Parameters:
        ref (int | str): index (0-based), label, or UniqueID.

    Uses:
        GAP.MOD[{model_name}].EQUIP[{ref}].MASK()
    """
    srv.do_cmd(f"GAP.MOD[{model_name}].EQUIP[{_wrap_idx_or_label(ref)}].MASK()")


def unmask(srv, ref: int | str, model_name="PROD"):
    """
    Unmask (open) a pipe or equipment by index, label, or UniqueID.

    Parameters:
        ref (int | str): index (0-based), label, or UniqueID.

    Uses:
        GAP.MOD[{model_name}].EQUIP[{ref}].UNMASK()
    """
    srv.do_cmd(f"GAP.MOD[{model_name}].EQUIP[{_wrap_idx_or_label(ref)}].UNMASK()")
