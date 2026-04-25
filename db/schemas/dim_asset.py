from sqlalchemy import Column,  DateTime, String, DateTime,Float
from sqlalchemy.dialects.postgresql import UUID
import uuid

from db.base_db import Base

class DimAsset(Base):
    __tablename__ = "dim_asset"

    asset_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(String)
    open_time = Column(DateTime)
    open_price = Column(Float)
    close_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    volume = Column(Float)