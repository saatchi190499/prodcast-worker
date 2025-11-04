import os
import datetime as dt
import csv
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
    ScenarioComponentLink,
    MainClass,
    ScenarioLog,
)
import requests

from worker.helpers import (
    build_unit_mapping,
    download_file,
    run_python_file,
    ensure_scenario_media_dir,
    log_scenario,
    excel_serial_date,
    convert_value_and_unit,
    download_component_file_to,
)

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

TARGET_UNIT_SYSTEM_NAME = os.getenv("UNIT_SYSTEM_NAME", "Norwegian S.I.")


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
"""
Scenario utilities and tasks
"""


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
            # Date as Excel serial
            date_val = excel_serial_date(getattr(row, "date_time", None))
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
            # Convert value and unit to target unit system
            value, unit, category = convert_value_and_unit(row, unit_map)
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

    # Download/copy the model file (with local fallback)
    if model_component_id:
        model_copied_path = download_component_file_to(folder, scenario_id, model_component_id, {".rsa"})

    return event_component_id, model_component_id, str(csv_path), model_copied_path


@shared_task(name="worker.run_scenario")
def run_scenario(scenario_id: int, start_date: str, end_date: str):
    """Run scenario job and prepare Events1.csv before execution."""
    scenario = ScenarioClass.objects.get(pk=scenario_id)
    task_id = current_task.request.id

    # Clean previous logs for this scenario at the start of each run
    try:
        ScenarioLog.objects.filter(scenario_id=scenario_id).delete()
    except Exception:
        # Do not fail the run if cleanup is not possible
        pass
    
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

                    # Extract the archive to the scenario media folder
                    rslv.extract_archive(srv, archive_file, str(extract_dir))

                    # Find a .rsl file in the extracted content
                    rsl_file: str | None = None
                    for p in extract_dir.rglob("*.rsl"):
                        rsl_file = str(p)
                        break

                    if not rsl_file:
                        raise RuntimeError(f"No .rsl file found after extracting {archive_file} to {extract_dir}")

                    log_scenario(scenario, f"Opening Resolve file: {rsl_file}", 55)

                    rslv.open_file(srv, rsl_file)

                    log_scenario(scenario, f"Running Resolve scenario: {scenario.scenario_name}", 70)

                    rslv.run_scenario(srv, "Scenario1")

                    # Check Resolve error state right after run
                    try:
                        if rslv.is_error(srv):
                            msg = rslv.error_msg(srv) or "(no message)"
                            rc = 1
                            log_scenario(scenario, f"Resolve error: {msg}", 85)
                        else:
                            log_scenario(scenario, "Resolve reports no errors", 88)
                    except Exception as e:
                        # If querying error state fails, log but continue to shutdown
                        log_scenario(scenario, f"Failed to query Resolve error state: {e}", 88)

                    log_scenario(scenario, "Resolve scenario completed", 90)

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
                log_scenario(scenario, f"Resolve automation failed: {e}", 40)

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
