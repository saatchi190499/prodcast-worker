import os
import sys
import csv
import tempfile
import shutil
import datetime as dt
from pathlib import Path
import subprocess

import requests
from django.utils import timezone
from django.conf import settings

from worker.models import (
    ScenarioLog,
    DataSourceComponent,
    UnitSystem,
    UnitSystemCategoryDefinition,
)


# -----------------
# Generic utilities
# -----------------
def build_unit_mapping(unit_system_name: str) -> tuple[object | None, dict[int, object]]:
    """
    Build a mapping from UnitCategory.id -> UnitDefinition for a given unit system.
    Returns (unit_system_obj_or_None, mapping_dict).
    """
    try:
        us = UnitSystem.objects.get(unit_system_name=unit_system_name)
    except UnitSystem.DoesNotExist:
        return None, {}

    pairs = (
        UnitSystemCategoryDefinition.objects
        .select_related("unit_category", "unit_definition")
        .filter(unit_system=us)
    )
    mapping = {p.unit_category_id: p.unit_definition for p in pairs}
    return us, mapping


def download_file(url: str) -> str:
    """Download a text file via HTTP to a temporary file and return its path."""
    r = requests.get(url, timeout=60, proxies={})
    r.raise_for_status()
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
        f.write(r.text)
    return tmp_path


def run_python_file(path: str, timeout: int = 3600):
    """
    Run a Python script ensuring project imports work and petex_client is available.
    Returns (returncode, stdout, stderr).
    """
    if not os.path.exists(path):
        return 127, "", f"File not found: {path}"

    # Read original code
    with open(path, "r", encoding="utf-8") as f:
        code = f.read()

    # Inject import header if missing
    if "from petex_client import" not in code and "import petex_client" not in code:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        header = (
            f"import sys, os; sys.path.insert(0, r'{base_dir}')\n"
            f"from petex_client import gap, gap_tools\n"
            f"from petex_client.utils import get_srv\n"
            f"srv = get_srv()\n"
        )
        footer = "srv.close()\n"
        code = header + "\n" + code + "\n" + footer

        temp_path = path + ".auto"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(code)
        path = temp_path

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    proc = subprocess.run(
        [sys.executable, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        cwd=project_root,
    )
    return proc.returncode, proc.stdout, proc.stderr


# ---------------------
# Scenario-level helpers
# ---------------------
def ensure_scenario_media_dir(scenario_id: int) -> Path:
    media_root = Path(getattr(settings, "MEDIA_ROOT", Path.cwd() / "media"))
    path = media_root / "scenarios" / str(scenario_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def log_scenario(scenario_id: int, message: str, progress: int | None = None) -> None:
    """Create a ScenarioLog entry; ignore failures to keep flow safe."""
    try:
        ScenarioLog.objects.create(
            scenario_id=scenario_id,
            timestamp=timezone.now(),
            message=message,
            progress=progress or 0,
        )
    except Exception:
        pass


def excel_serial_date(value) -> int:
    """Convert a datetime/date to Excel serial (date-only). Returns 0 if missing."""
    if not value:
        return 0
    try:
        if hasattr(value, "date"):
            try:
                if timezone.is_aware(value):
                    value = timezone.localtime(value)
            except Exception:
                pass
            d_only = value.date()
        else:
            d_only = value
        excel_epoch_date = dt.date(1899, 12, 30)
        return (d_only - excel_epoch_date).days
    except Exception:
        return 0


def convert_value_and_unit(row, unit_map: dict[int, object]) -> tuple[str, str, str]:
    """Convert `row.value` using unit_map; return (value, unit, category). Best-effort."""
    value = row.value or ""
    unit = ""
    category = (
        row.object_type_property.object_type_property_category
        if getattr(row, "object_type_property", None)
        else ""
    )

    target_ud = None
    if getattr(row, "object_type_property", None):
        uc_id = getattr(row.object_type_property, "unit_category_id", None)
        target_ud = unit_map.get(uc_id) if uc_id else None

    if target_ud:
        try:
            val_base = float(value)
            s = float(target_ud.scale_factor)
            o = float(target_ud.offset)
            val_target = (val_base - o) / s if s != 0 else val_base
            precision = getattr(target_ud, "precision", None)
            if isinstance(precision, int) and precision >= 0:
                value = f"{val_target:.{precision}f}"
            else:
                value = str(val_target)
            unit = target_ud.alias_text or target_ud.unit_definition_name or ""
        except Exception:
            unit = target_ud.alias_text or target_ud.unit_definition_name or ""
    else:
        if getattr(row, "object_type_property", None) and getattr(row.object_type_property, "unit", None):
            _unit = row.object_type_property.unit
            unit = _unit.alias_text or _unit.unit_definition_name or ""

    return str(value), unit, category


def download_component_file_to(
    folder: Path,
    scenario_id: int,
    component_id: int,
    allowed_exts: set[str] | None = None,
) -> str | None:
    """
    Download a component's file (via MEDIA url) into `folder`, restricting by extension.
    Attempts network download first, then falls back to local MEDIA_ROOT.
    Returns destination path or None.
    """
    allowed_exts = allowed_exts or {".rsa"}
    try:
        comp = DataSourceComponent.objects.get(pk=component_id)
    except DataSourceComponent.DoesNotExist:
        return None

    base = os.getenv("DJANGO_BASE_URL")
    file_field = getattr(comp, "file", None)
    if not base or not file_field:
        log_scenario(scenario_id, "Cannot download model: DJANGO_BASE_URL or component.file missing", 20)
        return None

    url = getattr(file_field, "url", None)
    name = getattr(file_field, "name", None)
    if not url and name:
        url = f"/media/{name}"
    if not url:
        log_scenario(scenario_id, "Model component file has no url/name to build download URL", 20)
        return None

    candidate_name = name if name else str(url)
    ext = Path(candidate_name).suffix.lower()
    if allowed_exts and ext not in allowed_exts:
        log_scenario(
            scenario_id,
            f"Model file skipped due to extension '{ext}'. Only {sorted(allowed_exts)} allowed.",
            20,
        )
        return None

    url_str = str(url)
    if url_str.startswith("http://") or url_str.startswith("https://"):
        full_url = url_str
    else:
        full_url = base.rstrip("/") + "/" + url_str.lstrip("/")

    # Try network download
    try:
        log_scenario(scenario_id, f"Downloading model file from {full_url}", 20)
        with requests.get(full_url, timeout=60, stream=True, proxies={"http": None, "https": None}) as r:
            r.raise_for_status()
            filename = Path(name).name if name else Path(url_str).name
            dst = folder / filename
            with open(dst, "wb") as out:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        out.write(chunk)
        log_scenario(scenario_id, f"Downloaded model file to {dst}", 22)
        return str(dst)
    except Exception as e:
        # Fallback to local MEDIA_ROOT
        media_root = Path(getattr(settings, "MEDIA_ROOT", Path.cwd() / "media"))
        rel_path = Path(name) if name else Path(url_str)
        local_candidate = media_root / rel_path
        if not local_candidate.exists():
            local_candidate = media_root / rel_path.name
        if local_candidate.exists():
            try:
                dst = folder / local_candidate.name
                shutil.copy2(local_candidate, dst)
                log_scenario(scenario_id, f"Copied model file from local MEDIA_ROOT: {local_candidate}", 22)
                return str(dst)
            except Exception:
                log_scenario(scenario_id, f"Failed to copy local model file: {local_candidate}", 22)
        else:
            log_scenario(scenario_id, f"Failed to download model file from {full_url}: {e}", 22)
        return None

