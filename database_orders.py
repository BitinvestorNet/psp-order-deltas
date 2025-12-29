from sqlalchemy import create_engine
import os
import pandas as pd
from config import HOURS_BACK_SEARCH
PORT = 5432
TABLE_NAME = "production"

engine = create_engine(f"postgresql://{os.getenv("SWAPPED_DB_USER")}:{os.getenv('SWAPPED_DB_PASS')}@{os.getenv("SWAPPED_DB_HOST")}:{PORT}/{TABLE_NAME}")

cols = [
    'order_id',
    'order_total',
    'order_currency',
    'payment_reference',
]

def read_from_db(hours_back: int = HOURS_BACK_SEARCH):
    cols_sql = ", ".join(cols)
    
    end = pd.Timestamp.now(tz='utc')
    start = end - pd.Timedelta(hours=hours_back)
    
    query = f"""
    SELECT {cols_sql} 
    FROM public.orders
    WHERE created_at >= %s
    AND created_at <= %s
    """
    
    df = pd.read_sql(query, engine, params=(start, end))
    df["order_id"] = df["order_id"].astype(str)
    return df

