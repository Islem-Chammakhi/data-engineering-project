from sqlalchemy import Column, Integer, String, TIMESTAMP
from db.base_db import Base

class IngestionRun(Base):
    __tablename__ = "ingestion_runs"

    run_id = Column(Integer, primary_key=True)
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    status = Column(String(20))
    total_tasks = Column(Integer)
    success_tasks = Column(Integer)
    failed_tasks = Column(Integer)