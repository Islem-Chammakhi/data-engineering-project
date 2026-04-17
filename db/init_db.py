from db.config_db import engine
from db.base_db import Base

from schemas.ingestion_run import IngestionRun
from schemas.ingestion_task import IngestionTask


def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Error initializing database: {e}")

init_db()