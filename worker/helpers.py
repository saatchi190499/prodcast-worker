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


def run_python_file(path: str, timeout: int = 3600, workflow_component_id: int | None = None):
    """
    Run a Python script ensuring project imports work and petex_client is available.
    Returns (returncode, stdout, stderr).
    """
    if not os.path.exists(path):
        return 127, "", f"File not found: {path}"

    # Read original code
    with open(path, "r", encoding="utf-8") as f:
        code = f.read()

    worker_dir = os.path.abspath(os.path.dirname(__file__))
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    # Inject workflow helpers into executed code
    base_header = (
        """import sys, os
import importlib.abc, importlib.util
sys.path.insert(0, r"{project_root}")
sys.path.insert(0, r"{worker_dir}")
MAIN_SERVER_URL = os.getenv("WORKER_MAIN_SERVER_URL")
if not MAIN_SERVER_URL:
    base = os.getenv("DJANGO_BASE_URL", "").rstrip("/")
    MAIN_SERVER_URL = f"{{base}}/api" if base else "http://localhost:8000/api"
MAIN_SERVER_MODULE_URL = f"{{MAIN_SERVER_URL}}/module"
DISABLE_REMOTE_IMPORTS = os.getenv("WORKER_DISABLE_REMOTE_IMPORTS", "").lower() in ("1", "true", "yes")
REMOTE_IMPORT_PREFIXES = [p.strip() for p in os.getenv("WORKER_REMOTE_IMPORT_PREFIXES", "apiapp,petex_client,pi_client").split(",") if p.strip()]
class RemoteModuleLoader(importlib.abc.SourceLoader):
    def __init__(self, fullname):
        self.fullname = fullname
    def get_data(self, path):
        try:
            import requests
        except ImportError as e:
            raise ImportError("requests is required for remote imports") from e
        module_path = path.replace(".", "/")
        url = f"{{MAIN_SERVER_MODULE_URL}}/{{module_path}}"
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404 and "/" not in module_path.split("/")[-1]:
            url = f"{{MAIN_SERVER_MODULE_URL}}/{{module_path}}/__init__.py"
            resp = requests.get(url, timeout=30)
        if resp.status_code != 200:
            raise ImportError(f"Failed to fetch: {{url}} ({{resp.status_code}})")
        return resp.text.encode("utf-8")
    def get_filename(self, fullname):
        return fullname
    def is_package(self, fullname):
        return fullname in REMOTE_IMPORT_PREFIXES
class RemoteModuleFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        for prefix in REMOTE_IMPORT_PREFIXES:
            if fullname == prefix or fullname.startswith(prefix + "."):
                loader = RemoteModuleLoader(fullname)
                return importlib.util.spec_from_loader(fullname, loader)
        return None
if not DISABLE_REMOTE_IMPORTS:
    sys.meta_path.insert(0, RemoteModuleFinder())
from worker.db import setup_django
setup_django()
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from worker.models import MainClass, DataSourceComponent, ObjectType, ObjectInstance, ObjectTypeProperty
from django.db import models as django_models
class MainClassHistory(django_models.Model):
    id = django_models.BigAutoField(primary_key=True)
    main_record = django_models.ForeignKey(
        MainClass, on_delete=django_models.CASCADE, related_name="history", db_index=True
    )
    time = django_models.DateTimeField("Time", db_index=True)
    value = django_models.TextField(db_column="value", null=True, blank=True)
    class Meta:
        db_table = "apiapp_mainclass_history"
        verbose_name = "Main Data Record History"
        verbose_name_plural = "Main Data Record History"
        app_label = "apiapp"
        ordering = ["-time"]
        indexes = [django_models.Index(fields=["main_record", "time"])]
def _resolve_obj(model, val, name_field, id_field):
    if val is None: return None
    if isinstance(val, dict):
        if val.get(id_field) is not None: return model.objects.get(pk=int(val[id_field]))
        if val.get("id") is not None: return model.objects.get(pk=int(val["id"]))
        if val.get(name_field): return model.objects.get(**{{name_field: val[name_field]}})
        if val.get("name"): return model.objects.get(**{{name_field: val["name"]}})
        return None
    if isinstance(val, int): return model.objects.get(pk=val)
    if isinstance(val, str): return model.objects.get(**{{name_field: val}})
    if hasattr(val, "pk"): return model.objects.get(pk=val.pk)
    return None
def _resolve_property(val, obj_type=None):
    if val is None: return None
    if isinstance(val, dict):
        if val.get("object_type_property_id") is not None: return ObjectTypeProperty.objects.get(pk=int(val["object_type_property_id"]))
        if val.get("id") is not None: return ObjectTypeProperty.objects.get(pk=int(val["id"]))
        name = val.get("object_type_property_name") or val.get("name")
        if name and obj_type: return ObjectTypeProperty.objects.get(object_type=obj_type, object_type_property_name=name)
        if name: return ObjectTypeProperty.objects.get(object_type_property_name=name)
        return None
    if isinstance(val, int): return ObjectTypeProperty.objects.get(pk=val)
    if isinstance(val, str):
        if obj_type: return ObjectTypeProperty.objects.get(object_type=obj_type, object_type_property_name=val)
        return ObjectTypeProperty.objects.get(object_type_property_name=val)
    if hasattr(val, "pk"): return ObjectTypeProperty.objects.get(pk=val.pk)
    return None
def workflow_save_output(records, component_id=None, mode="append", save_to=None):
    if save_to and str(save_to).lower() not in ("db", "database"):
        return {{"status": "skipped", "reason": "worker saves to db only"}}
    comp_id = component_id or os.getenv("WORKFLOW_COMPONENT_ID")
    if not comp_id: raise RuntimeError("workflow_save_output requires component_id or WORKFLOW_COMPONENT_ID")
    comp = DataSourceComponent.objects.get(pk=int(comp_id))
    items = records if isinstance(records, list) else [records]
    for rec in items:
        if rec is None: continue
        obj_type = _resolve_obj(ObjectType, rec.get("object_type"), "object_type_name", "object_type_id")
        obj_inst = _resolve_obj(ObjectInstance, rec.get("object_instance"), "object_instance_name", "object_instance_id")
        obj_prop = _resolve_property(rec.get("object_type_property"), obj_type)
        if not obj_type or not obj_inst or not obj_prop: raise RuntimeError("Missing object_type/object_instance/object_type_property")
        dt = rec.get("date_time")
        if isinstance(dt, str): dt = parse_datetime(dt)
        if dt is None: dt = timezone.now()
        data_set_id = rec.get("data_set_id")
        if mode == "replace":
            MainClass.objects.filter(component=comp, object_type=obj_type, object_instance=obj_inst, object_type_property=obj_prop).delete()
        obj = None
        if data_set_id:
            obj = MainClass.objects.get(pk=int(data_set_id), component=comp)
        else:
            obj = (
                MainClass.objects
                .filter(component=comp, object_type=obj_type, object_instance=obj_inst, object_type_property=obj_prop)
                .order_by("-data_set_id")
                .first()
            )
        if obj is None:
            obj = MainClass(component=comp, object_type=obj_type, object_instance=obj_inst, object_type_property=obj_prop)
        obj.value = rec.get("value")
        obj.tag = rec.get("tag")
        obj.description = rec.get("description")
        obj.date_time = dt
        obj.save()
        history_time = obj.date_time or timezone.now()
        last = (
            MainClassHistory.objects.filter(main_record=obj)
            .order_by("-time", "-id")
            .first()
        )
        if not (last and last.time == history_time and last.value == obj.value):
            MainClassHistory.objects.create(main_record=obj, time=history_time, value=obj.value)
    return {{"status": "saved_db", "count": len(items)}}
""".format(worker_dir=worker_dir, project_root=project_root)
    )

    petex_header = (
        f"from petex_client import gap, gap_tools\n"
        f"try:\n"
        f"    from petex_client.utils import get_srv\n"
        f"except Exception:\n"
        f"    def get_srv(allow_none=False):\n"
        f"        return None\n"
        f"srv = get_srv(allow_none=True)\n"
    )
    footer = "if 'srv' in globals() and srv is not None:\n    srv.close()\n"

    if "from petex_client import" not in code and "import petex_client" not in code:
        code = base_header + petex_header + "\n" + code + "\n" + footer
    else:
        code = base_header + "\n" + code

    temp_path = path + ".auto"
    with open(temp_path, "w", encoding="utf-8") as f:
        f.write(code)
    path = temp_path

    env = os.environ.copy()
    if workflow_component_id is not None:
        env["WORKFLOW_COMPONENT_ID"] = str(workflow_component_id)
    env["PYTHONPATH"] = f"{project_root}:{env.get('PYTHONPATH', '')}".rstrip(":")
    proc = subprocess.run(
        [sys.executable, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        cwd=project_root,
        env=env,
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

def normalize_ddmmyyyy(date_str: str) -> str:
    """Normalize various input date formats to dd/mm/yyyy.

    Accepts:
    - dd/mm/yyyy (e.g., 01/11/2025)
    - ISO date/time strings like 2025-11-01, 2025-11-01T13:53, 2025-11-01T13:53:00, with optional 'Z' or offset

    Returns the date as dd/mm/yyyy. Raises ValueError if parsing fails.
    """
    if date_str is None:
        return ""
    s = str(date_str).strip()
    if not s:
        return ""

    # Fast path: dd/mm/yyyy
    try:
        return dt.datetime.strptime(s, "%d/%m/%Y").strftime("%d/%m/%Y")
    except Exception:
        pass

    # Handle trailing 'Z'
    s_clean = s[:-1] if s.endswith("Z") else s

    # Try Python's ISO parser
    try:
        iso_dt = dt.datetime.fromisoformat(s_clean)
        return iso_dt.strftime("%d/%m/%Y")
    except Exception:
        pass

    # Try common variants
    fmts = [
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            d = dt.datetime.strptime(s_clean, fmt)
            return d.strftime("%d/%m/%Y")
        except Exception:
            continue

    raise ValueError(
        f"Invalid date '{date_str}'. Expected dd/mm/yyyy or ISO like 2025-11-01T13:53."
    )


def log_scenario(scenario: int, message: str, progress: int | None = None) -> None:
    """Create a ScenarioLog entry; ignore failures to keep flow safe."""
    try:
        ScenarioLog.objects.create(
            scenario=scenario,
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


def convert_value_and_unit(row, unit_map: dict[int, object], no_round: bool = False) -> tuple[str, str, str]:
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
            val_target = (val_base - o) * s if s != 0 else val_base
            if no_round:
                value = str(val_target)
            else:
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
        url = f"{name}"
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
