from sqlalchemy import Column, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
import uuid

from db.base_db import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    run_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    source_name = Column(String, nullable=False)

    status = Column(String, nullable=False)  # SUCCESS / FAILED / PARTIAL

    trigger_type = Column(String, default="manual")  # manual / scheduled

    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True), nullable=True)

    def __repr__(self):
        return f"<PipelineRun(run_id={self.run_id}, source={self.source_name}, status={self.status})>"