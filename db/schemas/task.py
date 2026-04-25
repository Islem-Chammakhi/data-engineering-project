from sqlalchemy import Column, String, DateTime, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid

from db.base_db import Base


class Task(Base):
    __tablename__ = "tasks"

    task_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    run_id = Column(UUID(as_uuid=True), ForeignKey("pipeline_runs.run_id"), nullable=False)

    task_name = Column(String, nullable=False)  
    # ingestion / transformation / future: validation, load ...

    status = Column(String, nullable=False)  
    # SUCCESS / FAILED / RETRY / SKIPPED

    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True), nullable=True)

    records_count = Column(Integer, default=0)

    error_message = Column(String, nullable=True)

    def __repr__(self):
        return f"<Task(task={self.task_name}, status={self.status}, run_id={self.run_id})>"