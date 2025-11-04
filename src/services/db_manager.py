from utils.utils import handle_large_values
from core.config import logger, settings
from core.db_config import Session
from schemas.models import (
    MainClass,
    ObjectInstance,
    ObjectTypeProperty,
    ScenarioClass,
)
from typing import Dict, List, Tuple, Optional
from datetime import datetime

def create_entry(
    scenario_id: int,
    component_id: int,
    object_type_id: int,
    object_instance_id: int,
    object_type_property_id: int,
    date_time,
    value: str,
    tag: Optional[str] = None,
    description: Optional[str] = None,
):
    return MainClass(
        scenario_id=scenario_id,
        component_id=component_id,
        object_type_id=object_type_id,
        object_instance_id=object_instance_id,
        object_type_property_id=object_type_property_id,
        date=date_time,
        value=str(value) if value is not None else None,
        tag=tag,
        description=description,
    )


def get_mappings(session):
    instance_mapping = {
        i.object_instance_name: (i.object_instance_id, i.object_type_id) for i in session.query(ObjectInstance).all()
    }
    property_mapping = {
        p.object_type_property_name: p.object_type_property_id for p in session.query(ObjectTypeProperty).all()
    }
    return instance_mapping, property_mapping


def _normalize_scenario_id_to_int(sid: str) -> int:
    s = (sid or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        raise ValueError(f"Cannot normalize scenario id to int: {sid}")
    return int(digits)


def delete_results_from_db(scenario_id: str):
    """Delete previous results for scenario only (by sc_id)."""
    session = Session()
    try:
        sc_id = _normalize_scenario_id_to_int(scenario_id)
        num_deleted = session.query(MainClass).filter(MainClass.scenario_id == sc_id).delete()
        session.commit()
        logger.info("%s entries deleted from the database (scenario=%s).", num_deleted, sc_id)
    except Exception:
        logger.error("An error occurred while deleting from the database:", exc_info=True)
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
            delete_results_from_db(scenario_id)

        sc_id = _normalize_scenario_id_to_int(scenario_id)
        component_id = None  # write by scenario only
        logger.info(f'[GAP] scenario={sc_id} timestep={timestep}')

        # маппинги из БД
        instance_mapping, property_mapping = get_mappings(session)
        
        # привязка свойств к названию входных серий (как вы описали)
        property_ids = {
            "str_gap_oil_rate": property_mapping.get("OilRate"),
            "str_gap_gas_rate": property_mapping.get("GasRate"),
            "str_gap_pcontrol": property_mapping.get("dPChoke"),
            "str_gap_pres": property_mapping.get("ReservoirPressure"),
            "str_gap_fwhp": property_mapping.get("WHPressure"),
            "str_gap_gor": property_mapping.get("GOR"),
            "str_gap_wc": property_mapping.get("WCT"),
            # drawdown можно сохранить как отдельное свойство, если у вас есть id:
            # при его отсутствии пропустим
            "str_gap_drawdown": property_mapping.get("Drawdown"),
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
            timestep_conv_str = timestep.split('_', 1)[1]
        except Exception:
            timestep_conv_str = timestep  # в крайнем случае сохраняем как есть
        # parse to datetime
        dt = None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d.%m.%Y"):
            try:
                dt = datetime.strptime(timestep_conv_str, fmt)
                break
            except Exception:
                continue
        if dt is None:
            dt = datetime.utcnow()

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
                        scenario_id=sc_id,
                        component_id=component_id,
                        object_type_id=object_type_id,
                        object_instance_id=object_instance_id,
                        object_type_property_id=prop_id,
                        date_time=dt,
                        value=str(value),
                        tag=unit_label,
                        description='',
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
