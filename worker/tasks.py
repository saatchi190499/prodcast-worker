from celery import shared_task, current_task
from django.utils import timezone
import os

from worker.db import setup_django
setup_django()

from worker.models import Workflow, WorkflowRun, ScenarioClass, ScenarioLog
import subprocess


def run_python_file(path: str, timeout: int = 3600):
    """
    Запуск скрипта из файла (для воркфлоу).
    """
    if not os.path.exists(path):
        return 127, "", f"File not found: {path}"

    proc = subprocess.run(
        ["python", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout
    )
    return proc.returncode, proc.stdout, proc.stderr


@shared_task(name="worker.run_workflow")
def run_workflow(workflow_id: int):
    wf = Workflow.objects.get(pk=workflow_id)

    run = WorkflowRun.objects.create(
        workflow=wf,
        task_id=current_task.request.id,
        status="STARTED",
        started_at=timezone.now(),
    )

    try:
        if not wf.code_file or not wf.code_file.path or not os.path.exists(wf.code_file.path):
            run.status = "ERROR"
            run.error = "No code file"
            run.finished_at = timezone.now()
            run.save()
            return {"status": "ERROR", "msg": "No code file"}

        rc, out, err = run_python_file(wf.code_file.path, timeout=7200)
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
