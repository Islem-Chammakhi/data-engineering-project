from sqlalchemy import Column,  DateTime, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid

from db.base_db import Base

class DimNews(Base):
    __tablename__ = "dim_news"

    news_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    title = Column(String)
    source = Column(String)
    description = Column(String)
    published_time = Column(DateTime)