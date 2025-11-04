import os
import datetime as dt
import csv
import subprocess
import tempfile
from pathlib import Path
import shutil

from celery import shared_task, current_task
from django.utils import timezone

from dotenv import load_dotenv
from worker.db import setup_django

# Initialize Django and env before importing Django-dependent modules
setup_django()
load_dotenv()

from django.conf import settings
from worker.models import (
    Workflow,
    WorkflowRun,
    ScenarioClass,
    ScenarioLog,
    ScenarioComponentLink,
    MainClass,
    DataSourceComponent,
    UnitSystem,
    UnitSystemCategoryDefinition,
)
import requests

# =========================
# Constants and utilities
# =========================

DEFAULT_WORKFLOW_TIMEOUT = 7200
EVENTS_CSV_NAME = "Events1.csv"
EVENTS_CSV_HEADER = [
    "Date",
    "Type",
    "Name",
    "Action",
    "Value",
    "Unit",
    "Category",
    "description",
]

# Preferred target unit system for export (can override via env)
TARGET_UNIT_SYSTEM_NAME = os.getenv("UNIT_SYSTEM_NAME", "Norwegian S.I.")

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
    """Скачиваем файл по URL во временный каталог и возвращаем путь."""
    # Bypass proxies for internal hosts
    r = requests.get(url, timeout=60, proxies={})
    r.raise_for_status()
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".py")
    with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
        f.write(r.text)
    return tmp_path

def run_python_file(path: str, timeout: int = 3600):
    """
    Runs a Python script (.py) safely, ensuring petex_client is importable
    even if executed from a temporary folder.

    Args:
        path (str): Path to the Python script.
        timeout (int): Execution timeout in seconds.

    Returns:
        tuple: (returncode, stdout, stderr)
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
        footer = (
            f"srv.close()\n"
        )
        code = header + "\n" + code + "\n" + footer

        # Create temporary file with patched imports
        temp_path = path + ".auto"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(code)
        path = temp_path

    # Run Python file from project root (so imports work)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    proc = subprocess.run(
        ["python", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
        cwd=project_root,  # 👈 ensures imports from petex_client work
    )

    return proc.returncode, proc.stdout, proc.stderr


# =========
# Workflows
# =========

@shared_task(name="worker.run_workflow")
def run_workflow(workflow_id: int, scheduler_id: int = None):
    """
    Run a workflow task, optionally linked to a WorkflowScheduler entry.
    """
    wf = Workflow.objects.get(pk=workflow_id)
    task_id = current_task.request.id

    try:
        # Try to update an existing queued run
        run = WorkflowRun.objects.get(task_id=task_id)
        run.status = "STARTED"
        run.started_at = timezone.now()
        run.save()
    except WorkflowRun.DoesNotExist:
        # fallback (manual run)
        run = WorkflowRun.objects.create(
            workflow=wf,
            scheduler_id=scheduler_id,  # ✅ safe, can be None
            task_id=task_id,
            status="STARTED",
            started_at=timezone.now(),
        )

    try:
        if not wf.code_file:
            run.status = "ERROR"
            run.error = "No code file"
            run.finished_at = timezone.now()
            run.save()
            return {"status": "ERROR", "msg": "No code file"}

        code_url = f"{os.getenv('DJANGO_BASE_URL')}/media/{wf.code_file.url}"
        local_path = download_file(code_url)

        rc, out, err = run_python_file(local_path, timeout=DEFAULT_WORKFLOW_TIMEOUT)
        run.finished_at = timezone.now()
        run.output = out[:5000]
        run.error = err[:5000] if rc != 0 else None
        run.status = "SUCCESS" if rc == 0 else "ERROR"
        run.save()

        return {"status": run.status}

    except Exception as e:
        run.status = "ERROR"
        run.error = str(e)
        run.finished_at = timezone.now()
        run.save()
        return {"status": "ERROR", "msg": str(e)}
# ======================
# Scenario: helpers/task
# ======================

def ensure_scenario_media_dir(scenario_id: int) -> Path:
    media_root = Path(getattr(settings, "MEDIA_ROOT", Path.cwd() / "media"))
    path = media_root / "scenarios" / str(scenario_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


# === Scenario utilities ===
def get_component_id_by_source_name(scenario_id: int, data_source_name: str) -> int | None:
    link = (
        ScenarioComponentLink.objects
        .select_related("component__data_source")
        .filter(
            scenario_id=scenario_id,
            component__data_source__data_source_name__iexact=data_source_name,
        )
        .first()
    )
    return link.component_id if link else None


def resolve_component_ids(scenario_id: int) -> tuple[int | None, int | None]:
    event_id = get_component_id_by_source_name(scenario_id, "Events")
    model_id = get_component_id_by_source_name(scenario_id, "Models")
    return event_id, model_id


def generate_events_csv_for_scenario(scenario_id: int) -> tuple[int | None, int | None, str, str | None]:
    """
    Find Event and Model component IDs for the scenario, create a media folder,
    and export MainClass rows for the Event component to Events1.csv.
    Also copies the linked Model component file into the same folder (if present).

    Returns: (event_component_id, model_component_id, csv_path, model_file_path or None)
    """
    event_component_id, model_component_id = resolve_component_ids(scenario_id)

    folder = ensure_scenario_media_dir(scenario_id)
    csv_path = folder / EVENTS_CSV_NAME
    model_copied_path: str | None = None

    # Query events data; if no event component, create empty file with header
    qs = MainClass.objects.none()
    if event_component_id:
        qs = (
            MainClass.objects
            .select_related("object_instance", "object_type", "object_type_property")
            .filter(component_id=event_component_id)
            .order_by("date_time")
        )

    # Prepare unit conversion mapping for target system
    _, unit_map = build_unit_mapping(TARGET_UNIT_SYSTEM_NAME)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EVENTS_CSV_HEADER)
        for row in qs.iterator():
            # Date as Excel serial number (date only); 0 if missing
            if row.date_time:
                dtv = row.date_time
                try:
                    if timezone.is_aware(dtv):
                        dtv = timezone.localtime(dtv)
                except Exception:
                    pass
                d_only = dtv.date()
                excel_epoch_date = dt.date(1899, 12, 30)
                date_val = (d_only - excel_epoch_date).days
            else:
                date_val = 0
            type_name = (
                row.object_type.object_type_name if getattr(row, "object_type", None) else ""
            )
            name = (
                row.object_instance.object_instance_name if getattr(row, "object_instance", None) else ""
            )
            # Prefer explicit tag as action; fallback to property name
            action = row.tag or (
                row.object_type_property.object_type_property_name
                if getattr(row, "object_type_property", None)
                else ""
            )
            # Convert value and unit to target unit system if mapping exists
            value = row.value or ""
            unit = ""
            target_ud = None
            if getattr(row, "object_type_property", None):
                uc_id = getattr(row.object_type_property, "unit_category_id", None)
                target_ud = unit_map.get(uc_id) if uc_id else None

            if target_ud:
                # Try numeric conversion from base unit to target
                try:
                    val_base = float(value)
                    s = float(target_ud.scale_factor)
                    o = float(target_ud.offset)
                    # Base -> target: target = (base - o)/s
                    val_target = (val_base - o) / s if s != 0 else val_base
                    # format using target precision if present
                    precision = getattr(target_ud, "precision", None)
                    if isinstance(precision, int) and precision >= 0:
                        fmt = f"{{:.{precision}f}}"
                        value = fmt.format(val_target)
                    else:
                        value = str(val_target)
                    unit = target_ud.alias_text or target_ud.unit_definition_name or ""
                except Exception:
                    # Fallback to original value; best-effort unit label
                    unit = target_ud.alias_text or target_ud.unit_definition_name or ""
            else:
                # No mapping: try to output base unit information if available
                if getattr(row, "object_type_property", None) and getattr(row.object_type_property, "unit", None):
                    _unit = row.object_type_property.unit
                    unit = _unit.alias_text or _unit.unit_definition_name or ""
            category = (
                row.object_type_property.object_type_property_category
                if getattr(row, "object_type_property", None)
                else ""
            )
            description = (row.description or "").replace("\n", " ")
            writer.writerow([
                date_val,
                type_name,
                name,
                action,
                value,
                unit,
                category,
                description,
            ])

    # Download the model file from the component only (no local copy branch)
    if model_component_id:
        try:
            comp = DataSourceComponent.objects.get(pk=model_component_id)
            # Download the component file via Django's MEDIA url
            base = os.getenv('DJANGO_BASE_URL')
            file_field = getattr(comp, 'file', None)
            if base and file_field:
                url = getattr(file_field, 'url', None)
                name = getattr(file_field, 'name', None)
                if not url and name:
                    url = f"/media/{name}"
                if url:
                    # Only allow .rsa files
                    candidate_name = name if name else str(url)
                    ext = Path(candidate_name).suffix.lower()
                    if ext != ".rsa":
                        ScenarioLog.objects.create(
                            scenario_id=scenario_id,
                            timestamp=timezone.now(),
                            message=f"Model file skipped due to extension '{ext}'. Only .rsa allowed.",
                            progress=20,
                        )
                    else:
                        # If url is absolute, use as-is; else join with base
                        url_str = str(url)
                        if url_str.startswith('http://') or url_str.startswith('https://'):
                            full_url = url_str
                        else:
                            full_url = base.rstrip('/') + '/' + url_str.lstrip('/')
                        try:
                            ScenarioLog.objects.create(
                                scenario_id=scenario_id,
                                timestamp=timezone.now(),
                                message=f"Downloading model file from {full_url}",
                                progress=20,
                            )
                            # Bypass environment proxies explicitly
                            r = requests.get(
                                full_url,
                                timeout=60,
                                stream=True,
                                proxies={"http": None, "https": None},
                            )
                            status = r.status_code
                            r.raise_for_status()
                            filename = Path(name).name if name else Path(url).name
                            dst = folder / filename
                            with open(dst, 'wb') as out:
                                for chunk in r.iter_content(chunk_size=8192):
                                    if chunk:
                                        out.write(chunk)
                            model_copied_path = str(dst)
                            ScenarioLog.objects.create(
                                scenario_id=scenario_id,
                                timestamp=timezone.now(),
                                message=f"Downloaded model file to {model_copied_path}",
                                progress=22,
                            )
                        except Exception as e:
                            # Try local MEDIA_ROOT fallback if download fails
                            media_root = Path(getattr(settings, 'MEDIA_ROOT', Path.cwd() / 'media'))
                            rel_path = Path(name) if name else Path(url_str)
                            local_candidate = media_root / rel_path  # MEDIA_ROOT/models_files/file.rsa
                            if not local_candidate.exists():
                                # Secondary: just basename under MEDIA_ROOT
                                local_candidate = media_root / rel_path.name
                            if local_candidate.exists():
                                try:
                                    dst = folder / local_candidate.name
                                    shutil.copy2(local_candidate, dst)
                                    model_copied_path = str(dst)
                                    ScenarioLog.objects.create(
                                        scenario_id=scenario_id,
                                        timestamp=timezone.now(),
                                        message=f"Copied model file from local MEDIA_ROOT: {local_candidate}",
                                        progress=22,
                                    )
                                except Exception:
                                    ScenarioLog.objects.create(
                                        scenario_id=scenario_id,
                                        timestamp=timezone.now(),
                                        message=f"Failed to copy local model file: {local_candidate}",
                                        progress=22,
                                    )
                            else:
                                ScenarioLog.objects.create(
                                    scenario_id=scenario_id,
                                    timestamp=timezone.now(),
                                    message=f"Failed to download model file from {full_url}: {e}",
                                    progress=22,
                                )
                else:
                    ScenarioLog.objects.create(
                        scenario_id=scenario_id,
                        timestamp=timezone.now(),
                        message="Model component file has no url/name to build download URL",
                        progress=20,
                    )
            else:
                ScenarioLog.objects.create(
                    scenario_id=scenario_id,
                    timestamp=timezone.now(),
                    message="Cannot download model: DJANGO_BASE_URL or component.file missing",
                    progress=20,
                )
        except DataSourceComponent.DoesNotExist:
            pass
        except Exception:
            # best-effort copy; ignore failures
            pass

    return event_component_id, model_component_id, str(csv_path), model_copied_path


@shared_task(name="worker.run_scenario")
def run_scenario(scenario_id: int, start_date: str, end_date: str):
    """Run scenario job and prepare Events1.csv before execution."""
    scenario = ScenarioClass.objects.get(pk=scenario_id)
    task_id = current_task.request.id

    ScenarioLog.objects.create(
        scenario=scenario,
        timestamp=timezone.now(),
        message=f"Task {task_id} STARTED. Range: {start_date} → {end_date}",
        progress=0,
    )

    try:
        event_id, model_id, csv_path, model_path = generate_events_csv_for_scenario(scenario_id)
        ScenarioLog.objects.create(
            scenario=scenario,
            timestamp=timezone.now(),
            message=(
                f"Prepared {EVENTS_CSV_NAME}; event_component_id={event_id}, "
                f"model_component_id={model_id}; path={csv_path}; model_file={model_path}"
            ),
            progress=25,
        )

        # If a model archive (.rsa) was downloaded, use Petex Resolve to extract, open and run
        rc = 0
        if model_path:
            try:
                ScenarioLog.objects.create(
                    scenario=scenario,
                    timestamp=timezone.now(),
                    message="Starting Resolve automation: extract archive, open file, run scenario",
                    progress=35,
                )

                # Lazy imports to avoid COM init at module import time
                from worker.petex_client.utils import get_srv
                from worker.petex_client import resolve as rslv

                # Prepare extraction directory
                archive_file = str(model_path)
                extract_dir = Path(csv_path).parent
                extract_dir.mkdir(parents=True, exist_ok=True)

                srv = None
                try:
                    srv = get_srv()
                    # Start Resolve if needed
                    try:
                        rslv.start(srv)
                    except Exception:
                        # continue if already started
                        pass

                    # Extract the archive to the scenario media folder/extracted
                    rslv.extract_archive(srv, archive_file, str(extract_dir))

                    # Find a .rsz file in the extracted content
                    rsl_file: str | None = None
                    for p in extract_dir.rglob("*.rsl"):
                        rsl_file = str(p)
                        break

                    if not rsl_file:
                        raise RuntimeError(f"No .rsl file found after extracting {archive_file} to {extract_dir}")

                    ScenarioLog.objects.create(
                        scenario=scenario,
                        timestamp=timezone.now(),
                        message=f"Opening Resolve file: {rsl_file}",
                        progress=55,
                    )

                    rslv.open_file(srv, rsl_file)

                    ScenarioLog.objects.create(
                        scenario=scenario,
                        timestamp=timezone.now(),
                        message=f"Running Resolve scenario: {scenario.scenario_name}",
                        progress=70,
                    )

                    rslv.run_scenario(srv, "Scenario1")

                    # Check Resolve error state right after run
                    try:
                        if rslv.is_error(srv):
                            msg = rslv.error_msg(srv) or "(no message)"
                            rc = 1
                            ScenarioLog.objects.create(
                                scenario=scenario,
                                timestamp=timezone.now(),
                                message=f"Resolve error: {msg}",
                                progress=85,
                            )
                        else:
                            ScenarioLog.objects.create(
                                scenario=scenario,
                                timestamp=timezone.now(),
                                message="Resolve reports no errors",
                                progress=88,
                            )
                    except Exception as e:
                        # If querying error state fails, log but continue to shutdown
                        ScenarioLog.objects.create(
                            scenario=scenario,
                            timestamp=timezone.now(),
                            message=f"Failed to query Resolve error state: {e}",
                            progress=88,
                        )

                    ScenarioLog.objects.create(
                        scenario=scenario,
                        timestamp=timezone.now(),
                        message="Resolve scenario completed",
                        progress=90,
                    )

                    # Shutdown Resolve gracefully
                    try:
                        rslv.shutdown(srv)
                    except Exception:
                        pass
                finally:
                    try:
                        if srv is not None:
                            srv.close()
                    except Exception:
                        pass
            except Exception as e:
                rc = 1
                ScenarioLog.objects.create(
                    scenario=scenario,
                    timestamp=timezone.now(),
                    message=f"Resolve automation failed: {e}",
                    progress=40,
                )

        ScenarioLog.objects.create(
            scenario=scenario,
            timestamp=timezone.now(),
            message=f"Task {task_id} finished with rc={rc}",
            progress=100,
        )

        scenario.status = "SUCCESS" if rc == 0 else "ERROR"
        scenario.save(update_fields=["status"])

        return {
            "status": scenario.status,
            "event_component_id": event_id,
            "model_component_id": model_id,
            "events_csv": str(csv_path),
            "model_file": model_path,
        }

    except Exception as e:
        ScenarioLog.objects.create(
            scenario=scenario,
            timestamp=timezone.now(),
            message=f"Task {task_id} ERROR: {e}",
            progress=0,
        )
        scenario.status = "ERROR"
        scenario.save(update_fields=["status"])
        return {"status": "ERROR", "msg": str(e)}
