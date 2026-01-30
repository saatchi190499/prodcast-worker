from __future__ import annotations

from worker.workflow_shared import (
    outputs_component_for,
    records_from_output_table,
    workflow_instances_from_config,
    workflow_properties_from_config,
)

import datetime as dt
import os
from dataclasses import dataclass

from django.db.models import Q
from django.utils import timezone

from worker.models import (
    DataSourceComponent,
    MainClass,
    ObjectInstance,
    ObjectType,
    ObjectTypeProperty,
    Workflow,
)

from django.db import models as django_models

class MainClassHistory(django_models.Model):
    id = django_models.BigAutoField(primary_key=True)
    main_record = django_models.ForeignKey(MainClass, on_delete=django_models.CASCADE, related_name="history", db_index=True)
    time = django_models.DateTimeField("Time", db_index=True)
    value = django_models.TextField(db_column="value", null=True, blank=True)

    class Meta:
        db_table = "apiapp_mainclass_history"
        app_label = "apiapp"
        ordering = ["-time"]



class AttrDict(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        self[name] = value


class _AutoRowList(AttrDict):
    def __getitem__(self, key):
        if key not in self:
            row = AttrDict({"ObjectInstance": None if isinstance(key, int) else key, "Sample": []})
            self[key] = row
        return dict.__getitem__(self, key)


class _AutoColumn(AttrDict):
    def __init__(self, prop, component_id=None):
        super().__init__({"ObjectTypeProperty": prop, "ComponentId": component_id, "Row": _AutoRowList(), "_row_list": []})


class _AutoTable(AttrDict):
    def __getitem__(self, key):
        if key not in self:
            self[key] = _AutoColumn(key, self.get("_ComponentId"))
        return dict.__getitem__(self, key)


def ensure_sample(samples, idx):
    while len(samples) <= idx:
        samples.append(AttrDict())
    return samples[idx]


def _parse_workflow_datetime(value):
    from django.utils.dateparse import parse_datetime

    if value is None:
        return None

    if isinstance(value, dt.datetime):
        return value if timezone.is_aware(value) else timezone.make_aware(value, timezone.get_current_timezone())

    if isinstance(value, dt.date):
        val = dt.datetime.combine(value, dt.time.min)
        return timezone.make_aware(val, timezone.get_current_timezone())

    s = str(value).strip()
    if not s:
        return None

    parsed = parse_datetime(s)
    if parsed is not None:
        return parsed if timezone.is_aware(parsed) else timezone.make_aware(parsed, timezone.get_current_timezone())

    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y"):
        try:
            val = dt.datetime.strptime(s, fmt)
            return timezone.make_aware(val, timezone.get_current_timezone())
        except Exception:
            continue

    return None


def _infer_object_type_from_name(name):
    if not isinstance(name, str):
        return None
    base = name
    if base.endswith("OutputsTable"):
        base = base[: -len("OutputsTable")]
    if base.endswith("InputsTable"):
        base = base[: -len("InputsTable")]
    base = base.strip()
    if not base:
        return None
    return base.replace("_", " ")


def _iter_output_rows(col):
    if not isinstance(col, dict):
        return []
    rows = col.get("_row_list")
    if isinstance(rows, list) and rows:
        return rows
    rows = col.get("Row")
    if isinstance(rows, list):
        return rows
    if isinstance(rows, dict):
        seen = set()
        out = []
        for row in rows.values():
            if id(row) in seen:
                continue
            seen.add(id(row))
            out.append(row)
        return out
    return []


def _split_ids_names(items, id_keys, name_keys):
    ids = []
    names = []
    for item in (items or []):
        if item is None:
            continue
        if isinstance(item, dict):
            for key in id_keys:
                if key in item and item[key] is not None:
                    ids.append(int(item[key]))
                    break
            else:
                for key in name_keys:
                    if key in item and item[key]:
                        names.append(str(item[key]))
                        break
                else:
                    raise ValueError(f"Unsupported dict for selection: {item}")
        elif hasattr(item, "pk"):
            ids.append(int(item.pk))
        elif isinstance(item, int):
            ids.append(item)
        elif isinstance(item, str):
            if item.isdigit():
                ids.append(int(item))
            else:
                names.append(item)
        else:
            raise ValueError(f"Unsupported selection item: {item!r}")
    return sorted(set(ids)), sorted(set(names))


def _apply_id_name_filter(qs, id_field, name_field, ids, names):
    if ids and names:
        return qs.filter(Q(**{f"{id_field}__in": ids}) | Q(**{f"{name_field}__in": names}))
    if ids:
        return qs.filter(**{f"{id_field}__in": ids})
    if names:
        return qs.filter(**{f"{name_field}__in": names})
    return qs


class InternalApi:
    def get_records(self, components=None, object_type=None, instances=None, properties=None):
        qs = MainClass.objects.select_related(
            "component",
            "object_type",
            "object_instance",
            "object_type_property",
        )
        # Mirror backend Internal query behavior
        qs = qs.filter(component__data_source__data_source_name__iexact="Internal")

        if components:
            ids, names = _split_ids_names(components, ("id", "component_id"), ("name", "component_name"))
            qs = _apply_id_name_filter(qs, "component_id", "component__name", ids, names)

        if object_type is not None:
            obj_items = object_type if isinstance(object_type, (list, tuple, set)) else [object_type]
            ids, names = _split_ids_names(obj_items, ("id", "object_type_id"), ("name", "object_type_name"))
            qs = _apply_id_name_filter(qs, "object_type_id", "object_type__object_type_name", ids, names)

        if instances:
            ids, names = _split_ids_names(instances, ("id", "object_instance_id"), ("name", "object_instance_name"))
            qs = _apply_id_name_filter(qs, "object_instance_id", "object_instance__object_instance_name", ids, names)

        if properties:
            ids, names = _split_ids_names(properties, ("id", "object_type_property_id"), ("name", "object_type_property_name"))
            qs = _apply_id_name_filter(qs, "object_type_property_id", "object_type_property__object_type_property_name", ids, names)

        qs = qs.order_by(
            "component_id",
            "object_instance__object_instance_name",
            "object_type_property__object_type_property_name",
            "date_time",
        )

        return list(
            qs.values(
                "data_set_id",
                "component_id",
                "component__name",
                "object_type_id",
                "object_type__object_type_name",
                "object_instance_id",
                "object_instance__object_instance_name",
                "object_type_property_id",
                "object_type_property__object_type_property_name",
                "value",
                "date_time",
                "tag",
                "description",
            )
        )

    def get_history(self, components=None, object_type=None, instances=None, properties=None, start=None, end=None):
        record_qs = MainClass.objects.select_related(
            "component",
            "object_type",
            "object_instance",
            "object_type_property",
        )

        # Mirror backend Internal query behavior (FIX)
        record_qs = record_qs.filter(component__data_source__data_source_name__iexact="Internal")

        if components:
            ids, names = _split_ids_names(components, ("id", "component_id"), ("name", "component_name"))
            record_qs = _apply_id_name_filter(record_qs, "component_id", "component__name", ids, names)

        if object_type is not None:
            obj_items = object_type if isinstance(object_type, (list, tuple, set)) else [object_type]
            ids, names = _split_ids_names(obj_items, ("id", "object_type_id"), ("name", "object_type_name"))
            record_qs = _apply_id_name_filter(record_qs, "object_type_id", "object_type__object_type_name", ids, names)

        if instances:
            ids, names = _split_ids_names(instances, ("id", "object_instance_id"), ("name", "object_instance_name"))
            record_qs = _apply_id_name_filter(record_qs, "object_instance_id", "object_instance__object_instance_name", ids, names)

        if properties:
            ids, names = _split_ids_names(properties, ("id", "object_type_property_id"), ("name", "object_type_property_name"))
            record_qs = _apply_id_name_filter(record_qs, "object_type_property_id", "object_type_property__object_type_property_name", ids, names)

        record_ids = record_qs.values_list("data_set_id", flat=True)
        qs = MainClassHistory.objects.select_related("main_record").filter(main_record_id__in=record_ids)

        if start:
            dt_val = _parse_workflow_datetime(start)
            if dt_val:
                qs = qs.filter(time__gte=dt_val)
        if end:
            dt_val = _parse_workflow_datetime(end)
            if dt_val:
                qs = qs.filter(time__lte=dt_val)

        qs = qs.order_by("time")

        return list(qs.values(
            "id",
            "main_record_id",
            "time",
            "value",
            "main_record__component_id",
            "main_record__object_type_id",
            "main_record__object_instance_id",
            "main_record__object_type_property_id",
        ))


    def _metadata(self):
        types = ObjectType.objects.all().order_by("object_type_name")
        instances = {}
        properties = {}
        for t in types:
            t_name = getattr(t, "object_type_name", None) or str(t)
            instances[t_name] = [
                {"id": i.object_instance_id, "name": i.object_instance_name}
                for i in ObjectInstance.objects.filter(object_type=t).order_by("object_instance_name")
            ]
            properties[t_name] = [
                {"id": p.object_type_property_id, "name": p.object_type_property_name}
                for p in ObjectTypeProperty.objects.filter(object_type=t).order_by("object_type_property_name")
            ]
        return {"instances": instances, "properties": properties}


internal = InternalApi()


def _get_workflow_config(kind):
    comp_id = os.getenv("WORKFLOW_COMPONENT_ID")
    try:
        comp_id_int = int(comp_id) if comp_id is not None else None
    except Exception:
        comp_id_int = None
    if not comp_id_int:
        return {}
    wf = Workflow.objects.filter(component_id=comp_id_int).first()
    if not wf:
        return {}
    if kind == "outputs":
        return getattr(wf, "outputs_config", None) or {}
    return getattr(wf, "inputs_config", None) or {}


def workflow_instances(object_type=None, kind="inputs"):
    cfg = _get_workflow_config(kind)
    return workflow_instances_from_config(cfg, object_type=object_type)



def workflow_properties(object_type=None, kind="inputs"):
    cfg = _get_workflow_config(kind)
    return workflow_properties_from_config(cfg, object_type=object_type)



def _outputs_component_for(object_type, prop):
    cfg = _get_workflow_config("outputs")
    return outputs_component_for(cfg, object_type, prop)


def _resolve_obj(model, val, name_field, id_field, *, obj_type=None):
    if val is None:
        return None
    if isinstance(val, dict):
        if val.get(id_field) is not None:
            return model.objects.get(pk=int(val[id_field]))
        if val.get("id") is not None:
            return model.objects.get(pk=int(val["id"]))
        if val.get(name_field):
            return model.objects.get(**{name_field: val[name_field]})
        return None
    if hasattr(val, "pk"):
        return model.objects.get(pk=val.pk)
    if isinstance(val, int):
        return model.objects.get(pk=val)
    if isinstance(val, str):
        if val.isdigit():
            return model.objects.get(pk=int(val))
        return model.objects.get(**{name_field: val})
    return None


def _resolve_property(val, obj_type):
    if val is None:
        return None
    if isinstance(val, dict):
        if val.get("object_type_property_id") is not None:
            return ObjectTypeProperty.objects.get(pk=int(val["object_type_property_id"]))
        if val.get("id") is not None:
            return ObjectTypeProperty.objects.get(pk=int(val["id"]))
        name = val.get("object_type_property_name") or val.get("name")
        if name:
            return ObjectTypeProperty.objects.get(object_type=obj_type, object_type_property_name=name)
        return None
    if hasattr(val, "pk"):
        return ObjectTypeProperty.objects.get(pk=val.pk)
    if isinstance(val, int):
        return ObjectTypeProperty.objects.get(pk=val)
    if isinstance(val, str):
        if obj_type:
            return ObjectTypeProperty.objects.get(object_type=obj_type, object_type_property_name=val)
        return ObjectTypeProperty.objects.get(object_type_property_name=val)
    return None


def _save_records_to_db(records, mode="append"):
    items = records if isinstance(records, list) else [records]
    comp_id_env = os.getenv("WORKFLOW_COMPONENT_ID")

    for rec in items:
        if not isinstance(rec, dict):
            continue

        comp_id = rec.get("component") or comp_id_env
        if not comp_id:
            raise RuntimeError("Missing component for record")
        comp = DataSourceComponent.objects.get(pk=int(comp_id))

        obj_type = _resolve_obj(ObjectType, rec.get("object_type"), "object_type_name", "object_type_id")
        obj_inst = _resolve_obj(ObjectInstance, rec.get("object_instance"), "object_instance_name", "object_instance_id")
        obj_prop = _resolve_property(rec.get("object_type_property"), obj_type)

        if not obj_type or not obj_inst or not obj_prop:
            raise RuntimeError("Missing object_type/object_instance/object_type_property")

        dt_val = _parse_workflow_datetime(rec.get("date_time"))
        if dt_val is None:
            dt_val = timezone.now()

        if mode == "replace":
            MainClass.objects.filter(component=comp, object_type=obj_type, object_instance=obj_inst, object_type_property=obj_prop).delete()

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
        obj.date_time = dt_val
        obj.save()

        history_time = obj.date_time or timezone.now()
        existing = MainClassHistory.objects.filter(main_record=obj, time=history_time).order_by("-id").first()
        if existing:
            if existing.value != obj.value:
                existing.value = obj.value
                existing.save(update_fields=["value"])
        else:
            MainClassHistory.objects.create(main_record=obj, time=history_time, value=obj.value)

    return {"status": "saved_db", "count": len(items)}


def workflow_save_output(records, mode="append", save_to=None, component_id=None):
    if save_to and str(save_to).lower() not in ("db", "database"):
        return {"status": "skipped", "reason": "worker saves to db only"}

    items = records if isinstance(records, list) else [records]
    for rec in items:
        if not isinstance(rec, dict):
            continue
        if rec.get("component") is None:
            rec["component"] = component_id or _outputs_component_for(rec.get("object_type"), rec.get("object_type_property"))
    return _save_records_to_db(items, mode=mode)


def workflow_result_save(table, mode="append", save_to=None):
    if save_to and str(save_to).lower() not in ("db", "database"):
        return {"status": "skipped", "reason": "worker saves to db only"}

    object_type = None
    if isinstance(table, str):
        object_type = _infer_object_type_from_name(table)
        table = globals().get(table)

    if isinstance(table, dict) and not object_type:
        object_type = table.get("_ObjectType") or table.get("ObjectType") or table.get("__object_type") or _infer_object_type_from_name(table.get("_TableName") or table.get("__table_name"))

    if isinstance(object_type, str):
        object_type = object_type.strip()


    outputs_cfg = _get_workflow_config("outputs")
    records = records_from_output_table(
        table,
        object_type=object_type,
        outputs_config=outputs_cfg,
        description=None,
        date_time=None,
    )
    return _save_records_to_db(records, mode=mode)


__all__ = [
    "AttrDict",
    "_AutoTable",
    "_AutoColumn",
    "_AutoRowList",
    "ensure_sample",
    "internal",
    "workflow_instances",
    "workflow_properties",
    "workflow_save_output",
    "workflow_result_save",
]
