import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def get_database_engine():
    """
    Constructs a highly optimized SQLAlchemy connection pool engine.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    
    if not all([user, password, host, port, db_name]):
        raise ValueError("Missing database environment connection parameters inside .env configuration context.")
        
    connection_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(connection_url, pool_size=5, max_overflow=10)

def load_dataframe_to_staging(df: pd.DataFrame, table_name: str, if_exists_rule: str = "append"):
    """
    Streams explicit execution slices straight into target relational tables
    utilizing bulk batch-processing configuration rules.
    """
    if df.empty:
        return
        
    engine = get_database_engine()
    with engine.begin() as connection:
        df.to_sql(
            name=table_name,
            con=connection,
            schema="public",
            if_exists=if_exists_rule,
            index=False,
            chunksize=1000,
            method="multi"
        )