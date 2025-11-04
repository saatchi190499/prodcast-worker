from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()


# ========= New schema (apiapp_*) =========

class MainClass(Base):
    __tablename__ = "apiapp_mainclass"

    data_set_id = Column(Integer, primary_key=True, autoincrement=True)
    scenario_id = Column(Integer, index=True, nullable=True)
    component_id = Column(Integer, index=True, nullable=True)
    object_type_id = Column(Integer, index=True)
    object_instance_id = Column(Integer)
    object_type_property_id = Column(Integer)
    value = Column(String(100), nullable=True)
    date = Column(DateTime, nullable=True)  # Django field is date_time with db_column='date'
    tag = Column(String(100), nullable=True)
    description = Column(Text, nullable=True)

    def __repr__(self):
        return (
            f"<MainClass(data_set_id={self.data_set_id}, scenario_id={self.scenario_id}, component_id={self.component_id},"
            f" object_type_id={self.object_type_id}, object_instance_id={self.object_instance_id}, object_type_property_id={self.object_type_property_id},"
            f" value={self.value}, date={self.date}, tag={self.tag})>"
        )


class ObjectInstance(Base):
    __tablename__ = "apiapp_object_instance"

    object_instance_id = Column(Integer, primary_key=True)
    object_instance_name = Column(String)
    object_type_id = Column(Integer)

    def __repr__(self):
        return (
            f"<ObjectInstance(object_instance_id={self.object_instance_id}, object_instance_name={self.object_instance_name},"
            f" object_type_id={self.object_type_id})>"
        )


class ObjectTypeProperty(Base):
    __tablename__ = "apiapp_object_type_property"

    object_type_property_id = Column(Integer, primary_key=True)
    object_type_property_name = Column(String)
    object_type_property_category = Column(String)
    object_type_id = Column(Integer)
    openserver = Column(String, nullable=True)
    unit_category_id = Column(Integer, nullable=True)
    unit_id = Column(Integer, nullable=True)

    def __repr__(self):
        return (
            f"<ObjectTypeProperty(id={self.object_type_property_id}, name={self.object_type_property_name},"
            f" category={self.object_type_property_category}, object_type_id={self.object_type_id})>"
        )


class ScenarioClass(Base):
    __tablename__ = "apiapp_scenarios"

    scenario_id = Column(Integer, primary_key=True)
    scenario_name = Column(String(50), nullable=False)
    description = Column(Text)
    status = Column(String(50))
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    created_date = Column(DateTime, nullable=True)

    def __repr__(self) -> str:
        return f"<ScenarioClass id={self.scenario_id} name={self.scenario_name} status={self.status}>"


class DataSource(Base):
    __tablename__ = "apiapp_data_source"

    id = Column(Integer, primary_key=True)
    data_source_name = Column(String(50), unique=True)
    data_source_type = Column(String(20))


class DataSourceComponent(Base):
    __tablename__ = "apiapp_data_source_component"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True)
    data_source_id = Column(Integer, ForeignKey("apiapp_data_source.id"))


class ScenarioComponentLink(Base):
    __tablename__ = "apiapp_scenario_component_link"

    id = Column(Integer, primary_key=True)
    scenario_id = Column(Integer, ForeignKey("apiapp_scenarios.scenario_id"))
    component_id = Column(Integer, ForeignKey("apiapp_data_source_component.id"))


class ScenarioLog(Base):
    __tablename__ = "apiapp_scenariolog"

    id = Column(Integer, primary_key=True, autoincrement=True)
    scenario_id = Column(Integer, ForeignKey("apiapp_scenarios.scenario_id"))
    timestamp = Column(DateTime(timezone=True))
    message = Column(Text)
    progress = Column(Integer)
