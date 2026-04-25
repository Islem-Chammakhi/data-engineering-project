from db.config_db import engine
from db.base_db import Base

from db.schemas.pipeline_run import PipelineRun
from db.schemas.task import Task
from db.schemas.dim_news import DimNews
from db.schemas.dim_asset import DimAsset
from db.schemas.fact_news_market import FactNewsMarket


def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")

init_db()