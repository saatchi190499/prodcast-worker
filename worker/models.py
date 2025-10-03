from django.db import models


class Workflow(models.Model):
    id = models.AutoField(primary_key=True)
    code_file = models.FileField(upload_to="workflows/", null=True, blank=True)

    class Meta:
        db_table = "apiapp_workflow"


class WorkflowRun(models.Model):
    id = models.AutoField(primary_key=True)
    workflow = models.ForeignKey(Workflow, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=255, null=True, blank=True)
    status = models.CharField(max_length=20, default="PENDING")
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    output = models.TextField(null=True, blank=True)
    error = models.TextField(null=True, blank=True)

    class Meta:
        db_table = "apiapp_workflowrun"


class ScenarioClass(models.Model):
    scenario_id = models.AutoField(primary_key=True)
    scenario_name = models.CharField(max_length=50)
    status = models.CharField(max_length=50)

    class Meta:
        db_table = "apiapp_scenarios"


class ScenarioLog(models.Model):
    id = models.AutoField(primary_key=True)
    scenario = models.ForeignKey(ScenarioClass, on_delete=models.CASCADE)
    timestamp = models.DateTimeField(auto_now_add=True)
    message = models.TextField()
    progress = models.IntegerField(default=0)

    class Meta:
        db_table = "apiapp_scenariolog"
