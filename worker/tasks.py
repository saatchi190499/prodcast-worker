
import os
from celery import shared_task, current_task
from django.utils import timezone

from dotenv import load_dotenv
from worker.db import setup_django
import subprocess
setup_django()
load_dotenv()
from worker.models import Workflow, WorkflowRun, ScenarioClass, ScenarioLog
import requests
import tempfile

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

        rc, out, err = run_python_file(local_path, timeout=7200)
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




@shared_task(name="worker.run_scenario")
def run_scenario(scenario_id: int, start_date: str, end_date: str):
    scenario = ScenarioClass.objects.get(pk=scenario_id)
    task_id = current_task.request.id

    ScenarioLog.objects.create(
        scenario=scenario,
        timestamp=timezone.now(),
        message=f"Task {task_id} STARTED. Range: {start_date} → {end_date}",
        progress=0,
    )

    try:
        # Здесь вставишь реальный запуск GAP/Resolve
        rc, out, err = 0, "Scenario done", ""

        ScenarioLog.objects.create(
            scenario=scenario,
            timestamp=timezone.now(),
            message=f"Task {task_id} finished with rc={rc}",
            progress=100,
        )

        scenario.status = "SUCCESS" if rc == 0 else "ERROR"
        scenario.save(update_fields=["status"])

        return {"status": scenario.status}

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
