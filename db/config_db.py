import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()

DB_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DB_URL,
    # pool_size=5, # un ensemble de connexions déjà ouvertes, prêtes à être réutilisées. 5 requêtes peuvent être traitées en parallèle
    # max_overflow=10,
    echo=True  
)
