from sqlalchemy import Column,  Float,  Boolean, DateTime, ForeignKey, DateTime
from sqlalchemy.dialects.postgresql import UUID
import uuid

from db.base_db import Base

class FactNewsMarket(Base):
    __tablename__ = "fact_news_market"

    fact_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    news_id = Column(UUID(as_uuid=True), ForeignKey("dim_news.news_id"), nullable=False)
    asset_id = Column(UUID(as_uuid=True), ForeignKey("dim_asset.asset_id"), nullable=False)

    event_time = Column(DateTime)
    asset_time_at_event = Column(DateTime)

    price_at_event = Column(Float)
    price_1h = Column(Float)
    price_4h = Column(Float)
    price_24h = Column(Float)

    return_1h = Column(Float)
    return_4h = Column(Float)
    return_24h = Column(Float)

    spike_flag = Column(Boolean)