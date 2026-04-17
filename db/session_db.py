from sqlalchemy.orm import sessionmaker
from db.config_db import engine

SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False
)

def get_session():
    return SessionLocal()