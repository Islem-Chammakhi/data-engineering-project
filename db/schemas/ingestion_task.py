from sqlalchemy import Column, Integer, String, TIMESTAMP, Text, ForeignKey
from db.base_db import Base

class IngestionTask(Base):
    __tablename__ = "ingestion_tasks"

    task_id = Column(Integer, primary_key=True)
    run_id = Column(Integer, ForeignKey("ingestion_runs.run_id"))
    source = Column(String(50))
    status = Column(String(20))
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    error_message = Column(Text)
    records_count = Column(Integer)
    last_timestamp = Column(TIMESTAMP)