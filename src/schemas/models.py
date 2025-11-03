from sqlalchemy import Column, String, Integer, DECIMAL, Text, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class MainClass(Base):
    __tablename__ = "catalogapp_mainclass"

    id = Column(Integer, primary_key=True, autoincrement=True)
    data_source_type = Column(String, index=True)
    data_source_id = Column(String, index=True)
    object_type_id = Column(String, index=True)
    object_instance_id = Column(Integer)
    object_type_property_id = Column(Integer)
    value = Column(DECIMAL(precision=30, scale=20))
    date = Column(String)
    sub_data_source = Column(String)
    description = Column(String)

    def __repr__(self):
        return (f"<MainClass(id={self.id}, data_source_type={self.data_source_type}, data_source_id={self.data_source_id},"
                f" object_type_id={self.object_type_id}, object_instance_id={self.object_instance_id}, object_type_property_id={self.object_type_property_id},"
                f" value={self.value}, date={self.date}, sub_data_source={self.sub_data_source}, description={self.description})>")

class ObjectInstance(Base):
    __tablename__ = "catalogapp_objectinstance"

    object_instance_id = Column(Integer, primary_key=True)
    object_instance_name = Column(String)
    object_type_id = Column(Integer)

    def __repr__(self):
        return f"<ObjectInstance(object_instance_id={self.object_instance_id}, object_instance_name={self.object_instance_name}, object_type_id={self.object_type_id})>"

class ObjectTypeProperty(Base):
    __tablename__ = "catalogapp_objecttypeproperty"

    object_type_property_id = Column(Integer, primary_key=True)
    object_type_property_name = Column(String)
    object_type_property_category = Column(String)
    object_type_id = Column(Integer)

    def __repr__(self):
        return (f"<ObjectTypeProperty(object_type_property_id={self.object_type_property_id}, object_type_property_name={self.object_type_property_name},"
                f" object_type_property_category={self.object_type_property_category}, object_type_id={self.object_type_id})>")



class ScenarioJob(Base):
    __tablename__ = "catalogapp_scenariojob"
    id = Column(Integer, primary_key=True)
    celery_id = Column(String(255), index=True)
    state = Column(String(16))
    progress = Column(Integer)
    message = Column(Text)
    selected_server_id = Column(Integer, ForeignKey("catalogapp_serversclass.id"), nullable=True)
    created_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    result_path = Column(Text)
    scenario_id = Column(Text)

class ServersClass(Base):
    __tablename__ = "catalogapp_serversclass"
    id = Column(Integer, primary_key=True)
    server_name = Column(String(255), unique=True)


class ScenarioClass(Base):
    __tablename__ = "catalogapp_scenarioclass"
    scenario_id = Column(String(50), primary_key=True)   # PK в Django
    scenario_name = Column(String(50), nullable=False)

    created_date = Column(DateTime)
    start_date   = Column(DateTime, nullable=True)
    end_date     = Column(DateTime, nullable=True)

    status      = Column(String(50))     # ← будем обновлять
    description = Column(Text)           # ← можно обновлять по желанию

    # FK-холдеры (как обычные колонки, без ForeignKey — они не нужны для апдейта)
    server_id            = Column(Integer, nullable=True)
    models_id_id         = Column(Integer, nullable=True)
    trends_set_id_id     = Column(String(50), nullable=True)
    events_set_id_id     = Column(String(50), nullable=True)
    fixed_wells_set_id_id= Column(Integer, nullable=True)
    created_by_id        = Column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"<ScenarioClass id={self.scenario_id} name={self.scenario_name} status={self.status}>"