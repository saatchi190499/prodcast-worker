from utils.utils import convert_input_data, handle_large_values
from core.config import logger, settings
from core.db_config import Session
from core.const import NAME_MAPPING
from schemas.models import MainClass, ObjectInstance, ObjectTypeProperty, ScenarioJob
from typing import Dict, List, Tuple
from .scenario_status import set_scenario_status

def create_entry(
        data_source_type: str, 
        data_source_id: str, 
        object_type_id: int, 
        object_instance_id: int, 
        object_type_property_id: int, 
        date: str, 
        value: float, 
        sub_data_source: str, 
        description: str
    ):
    return MainClass(
        data_source_type=data_source_type,
        data_source_id=data_source_id,
        object_type_id=object_type_id,
        object_instance_id=object_instance_id,
        object_type_property_id=object_type_property_id,
        date=date,
        value=float(value),
        sub_data_source=sub_data_source,
        description=description
    )


def get_mappings(session):
    instance_mapping = {
        i.object_instance_name: (i.object_instance_id, i.object_type_id) for i in session.query(ObjectInstance).all()
    }
    property_mapping = {
        p.object_type_property_name: p.object_type_property_id for p in session.query(ObjectTypeProperty).all()
    }
    return instance_mapping, property_mapping


def delete_results_from_db(
        # scenario_id: int,
        scenario_id: str,
        data_source_type: str
    ):
    session = Session()
    # data_source_id = data_source_type + str(scenario_id)
    data_source_id = str(scenario_id)
    print(f'delete {data_source_id}')
    try:
        num_deleted = session.query(MainClass).filter_by(data_source_id=data_source_id).delete()
        session.commit()
        logger.info(f"{num_deleted} entries deleted from the database.")
    except Exception as e:
        logger.error("An error occurred while deleting from the database:", exc_info=True)
        session.rollback()
    finally:
        session.close()


def save_process_results(
        scenario_id: str,
        keys: str,
        values: str,
        timestep: str,
        current_timestep: str,
        celery_id: str
    ):
    session = Session()

    if current_timestep == 'timestep_0':
        delete_results_from_db(scenario_id, 'RS')

    data_source_type = 'RS'
    # data_source_id = data_source_type + str(scenario_id)
    data_source_id = str(scenario_id)
    print(f'process {data_source_id}')
    converted_data = convert_input_data(keys, values, timestep)

    instance_mapping, property_mapping = get_mappings(session)

    existing_entries = session.query(MainClass).filter_by(data_source_id=data_source_id).all()
    existing_keys = {
        (entry.object_instance_id, entry.object_type_property_id, entry.date)
        for entry in existing_entries
    }

    entries = []
    missing_keys = set()
    missing_instances = set()

    for key, value in converted_data.items():
        if key == "timestep":
            continue

        object_instance_info = NAME_MAPPING.get(key)

        if object_instance_info is None:
            continue

        object_instance_name = object_instance_info["instance_name"]
        property_name = object_instance_info["property_name"]

        if object_instance_name not in instance_mapping:
            missing_instances.add(object_instance_name)
            continue

        instance_id, obj_type_id = instance_mapping[object_instance_name]
        property_id = property_mapping.get(property_name)

        if not property_id:
            missing_keys.add(key)
            continue

        if (instance_id, property_id, timestep) not in existing_keys:
            entries.append(
                create_entry(
                    data_source_type,
                    data_source_id,
                    obj_type_id,
                    instance_id,
                    property_id,
                    converted_data['time'],
                    value,
                    'test',
                    ''
                )
            )
    try:
        if entries:
            session.bulk_save_objects(entries)

        updates = [{
            "id": celery_id,         
            "message": timestep,     
            "state": "PROGRESS",     
        }]

        session.bulk_update_mappings(ScenarioJob, updates)
        session.commit()
        
        set_scenario_status(scenario_id, timestep)
        logger.info(
            f"{len(entries)} new entries saved; message updated for job {celery_id}."
            if entries else
            f"No new entries; message updated for job {celery_id}."
        )

    except Exception as e:
        logger.info(f"Failed to save results to the database: {e}")
        session.rollback()
    finally:
        session.close()




def _split_pipeline(s: str) -> List[str]:
    if not s:
        return []
    parts = s.split('|')
    if parts and parts[-1] == '':
        parts = parts[:-1]
    return parts

def _parse_series(series_str: str) -> List[str]:
    # применяем вашу нормализацию (3.4e+35 и пр.)
    return [handle_large_values(x) for x in _split_pipeline(series_str)]

def save_gap_results(
        scenario_id: str,
        timestep: str,
        wells: str,
        separators: str,
        current_timestep: str,
        str_gap_gor: str,
        str_gap_gas_rate: str,
        str_gap_oil_rate: str,
        str_gap_drawdown: str,
        str_gap_pres: str,
        str_gap_wc: str,
        str_gap_fwhp: str,
        str_gap_pcontrol: str
):
    session = Session()
    try:
        if current_timestep == 'timestep_0':
            delete_results_from_db(scenario_id, 'GA')

        data_source_type = 'GA'
        data_source_id = str(scenario_id)
        logger.info(f'[GAP] scenario={data_source_id} timestep={timestep}')

        # маппинги из БД
        instance_mapping, property_mapping = get_mappings(session)

        # привязка свойств к названию входных серий (как вы описали)
        property_ids = {
            "str_gap_oil_rate": property_mapping.get("Oil_rate"),
            "str_gap_gas_rate": property_mapping.get("Gas_rate"),
            "str_gap_pcontrol": property_mapping.get("dPChoke"),
            "str_gap_pres": property_mapping.get("Reservoir_pressure"),
            "str_gap_fwhp": property_mapping.get("WHP_bar"),
            "str_gap_gor": property_mapping.get("GOR"),
            "str_gap_wc": property_mapping.get("water_cut"),
            # drawdown можно сохранить как отдельное свойство, если у вас есть id:
            # при его отсутствии пропустим
            "str_gap_drawdown": property_mapping.get("drawdown"),
        }

        # списки скважин и их "юнитов" (сепараторов)
        well_list = _split_pipeline(wells)
        separator_list = _split_pipeline(separators)
        if len(separator_list) < len(well_list):
            # подстрахуемся, чтобы zip не потерял хвосты
            separator_list += ['Unknown'] * (len(well_list) - len(separator_list))
        routing_dict: Dict[str, str] = dict(zip(well_list, separator_list))

        # преобразуем timestep 'timestep_01/01/2025' -> '01/01/2025'
        try:
            timestep_conv = timestep.split('_', 1)[1]
        except Exception:
            timestep_conv = timestep  # в крайнем случае сохраняем как есть

        # подготовим серии значений
        series_map: Dict[str, List[str]] = {
            "str_gap_gor": _parse_series(str_gap_gor),
            "str_gap_gas_rate": _parse_series(str_gap_gas_rate),
            "str_gap_oil_rate": _parse_series(str_gap_oil_rate),
            "str_gap_drawdown": _parse_series(str_gap_drawdown),
            "str_gap_pres": _parse_series(str_gap_pres),
            "str_gap_wc": _parse_series(str_gap_wc),
            "str_gap_fwhp": _parse_series(str_gap_fwhp),
            "str_gap_pcontrol": _parse_series(str_gap_pcontrol),
        }

        # найдём минимальную длину по всем рядам, чтобы не вылезти за пределы
        lengths = [len(well_list)] + [len(v) for v in series_map.values()]
        min_len = min(lengths) if lengths else 0
        if any(l != min_len for l in lengths if l):  # только если что-то отличается
            logger.warning(
                f'[GAP] length mismatch: wells={len(well_list)}, '
                f"gor={len(series_map['str_gap_gor'])}, gas={len(series_map['str_gap_gas_rate'])}, "
                f"oil={len(series_map['str_gap_oil_rate'])}, dd={len(series_map['str_gap_drawdown'])}, "
                f"pres={len(series_map['str_gap_pres'])}, wc={len(series_map['str_gap_wc'])}, "
                f"fwhp={len(series_map['str_gap_fwhp'])}, pctl={len(series_map['str_gap_pcontrol'])}. "
                f'Will truncate to {min_len}.'
            )

        # обрежем всё к min_len
        well_list = well_list[:min_len]
        for k in series_map:
            series_map[k] = series_map[k][:min_len]

        entries = []
        # для каждой скважины добавим записи по каждому свойству
        for idx, well in enumerate(well_list):
            inst_tuple = instance_mapping.get(well)  # (object_instance_id, object_type_id)
            if not inst_tuple:
                # нет в справочнике объектов — пропускаем
                continue
            object_instance_id, object_type_id = inst_tuple
            unit_label = routing_dict.get(well, 'Unknown')

            # собираем значение→property_id
            per_well_values: List[Tuple[str, int]] = []
            for series_name, values_list in series_map.items():
                prop_id = property_ids.get(series_name)
                if not prop_id:
                    # нет такого свойства в БД — пропустим
                    continue
                value = values_list[idx]
                per_well_values.append((value, prop_id))

            # создаём entries
            for value, prop_id in per_well_values:
                entries.append(
                    create_entry(
                        data_source_type=data_source_type,
                        data_source_id=data_source_id,
                        object_type_id=object_type_id,
                        object_instance_id=object_instance_id,
                        object_type_property_id=prop_id,
                        date=timestep_conv,
                        value=value,
                        sub_data_source=unit_label,
                        description=''
                    )
                )

        if not entries:
            logger.warning('[GAP] nothing to save: empty entries set')
            return

        session.bulk_save_objects(entries)
        session.commit()
        logger.info(f'[GAP] saved {len(entries)} entries to database.')

    except Exception:
        logger.error('Error while saving GAP results', exc_info=True)
        session.rollback()
        raise
    finally:
        session.close()