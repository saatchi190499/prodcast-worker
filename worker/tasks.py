import os
import csv
import subprocess
import tempfile
from pathlib import Path

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
)
import requests

# =========================
# Constants and utilities
# =========================

DEFAULT_WORKFLOW_TIMEOUT = 7200
EVENTS_CSV_NAME = "Events1.csv"
EVENTS_CSV_HEADER = [
    "date_time",
    "object_type_id",
    "object_instance_id",
    "object_type_property_id",
    "tag",
    "value",
    "description",
]


def download_file(url: str) -> str:
    """Скачиваем файл по URL во временный каталог и возвращаем путь."""
    r = requests.get(url, timeout=60)
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
def get_component_id_by_component_name(scenario_id: int, component_name: str) -> int | None:
    link = (
        ScenarioComponentLink.objects
        .select_related("component")
        .filter(
            scenario_id=scenario_id,
            component__name__iexact=component_name,
        )
        .first()
    )
    return link.component_id if link else None


def resolve_component_ids(scenario_id: int) -> tuple[int | None, int | None]:
    event_id = get_component_id_by_component_name(scenario_id, "Event")
    model_id = get_component_id_by_component_name(scenario_id, "Model")
    return event_id, model_id


def generate_events_csv_for_scenario(scenario_id: int) -> tuple[int | None, int | None, str]:
    """
    Find Event and Model component IDs for the scenario, create a media folder,
    and export MainClass rows for the Event component to Events1.csv.

    Returns: (event_component_id, model_component_id, csv_path)
    """
    event_component_id, model_component_id = resolve_component_ids(scenario_id)

    folder = ensure_scenario_media_dir(scenario_id)
    csv_path = folder / EVENTS_CSV_NAME

    # Query events data; if no event component, create empty file with header
    qs = MainClass.objects.none()
    if event_component_id:
        qs = (
            MainClass.objects
            .select_related("object_instance", "object_type", "object_type_property")
            .filter(scenario_id=scenario_id, component_id=event_component_id)
            .order_by("date_time")
        )

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(EVENTS_CSV_HEADER)
        for row in qs.iterator():
            writer.writerow([
                row.date_time.isoformat() if row.date_time else "",
                row.object_type_id,
                row.object_instance_id,
                row.object_type_property_id,
                row.tag or "",
                row.value or "",
                (row.description or "").replace("\n", " "),
            ])

    return event_component_id, model_component_id, str(csv_path)


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
        event_id, model_id, csv_path = generate_events_csv_for_scenario(scenario_id)
        ScenarioLog.objects.create(
            scenario=scenario,
            timestamp=timezone.now(),
            message=(
                f"Prepared {EVENTS_CSV_NAME}; event_component_id={event_id}, "
                f"model_component_id={model_id}; path={csv_path}"
            ),
            progress=25,
        )
        
        # Actual scenario logic placeholder
        rc = 0

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
