from django.db import models


# === Основная таблица Workflow ===
class Workflow(models.Model):
    id = models.AutoField(primary_key=True)
    code_file = models.FileField(upload_to="workflows/", null=True, blank=True)

    class Meta:
        db_table = "apiapp_workflow"


# === История запусков (лог работы воркфлоу) ===
class WorkflowRun(models.Model):
    workflow = models.ForeignKey("Workflow", on_delete=models.CASCADE, related_name="runs")
    scheduler = models.ForeignKey(
        "WorkflowScheduler",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="runs"
    )
    task_id = models.CharField(max_length=255, null=True, blank=True)  # Celery task ID
    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=50, default="QUEUED")  # QUEUED, SUCCESS, ERROR
    output = models.TextField(blank=True, null=True)   # stdout
    error = models.TextField(blank=True, null=True)    # stderr

    class Meta:
        db_table = "apiapp_workflow_run"
        ordering = ["-started_at"]

    def __str__(self):
        return f"Workflow {self.workflow_id} run @ {self.started_at} — {self.status}"

# === Планировщик воркфлоу ===
class WorkflowScheduler(models.Model):
    id = models.AutoField(primary_key=True)
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE)
    cron_expression = models.CharField(max_length=100)
    next_run = models.DateTimeField(null=True, blank=True)
    last_run = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "apiapp_workflow_scheduler"


# === Логи планировщика воркфлоу ===
class WorkflowSchedulerLog(models.Model):
    id = models.AutoField(primary_key=True)
    scheduler = models.ForeignKey(WorkflowScheduler, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=50)
    message = models.TextField()

    class Meta:
        db_table = "apiapp_workflow_scheduler_log"


# === Класс сценариев ===
class ScenarioClass(models.Model):
    scenario_id = models.AutoField(primary_key=True)
    scenario_name = models.CharField(max_length=50)
    status = models.CharField(max_length=50)

    class Meta:
        db_table = "apiapp_scenarios"   # ⚠️ должно совпадать с таблицей в основной базе


# === Логи выполнения сценариев ===
class ScenarioLog(models.Model):
    id = models.AutoField(primary_key=True)
    scenario = models.ForeignKey(ScenarioClass, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    message = models.TextField()
    progress = models.IntegerField(default=0)

    class Meta:
        db_table = "apiapp_scenariolog"


# === (опционально) Список серверов (если нужно для мониторинга) ===
class ServersClass(models.Model):
    id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=100)
    ip_address = models.CharField(max_length=100, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    allow_workflows = models.BooleanField(default=True)
    allow_scenarios = models.BooleanField(default=True)

    class Meta:
        db_table = "apiapp_servers"
