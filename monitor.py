import pandas as pd
from payment_providers import PaymentMonitor
from database_orders import read_from_db
from config import HOURS_BACK_SEARCH
import numpy as np

def monitor_deltas(hours_back: int = HOURS_BACK_SEARCH, delta_threshold: float = 0.001) -> pd.DataFrame:
    """Fetch payments, match orders, detect mismatches."""
    # Fetch data
    monitor = PaymentMonitor()
    df_payments = monitor.fetch_all_payments(hours_back=hours_back)
    orders_db = read_from_db()
    
    df_non_januar = df_payments[df_payments.psp != "januar"].merge(
        orders_db, on="order_id", how="left", suffixes=('', '_db')
    )
    df_januar = df_payments[df_payments.psp == "januar"].merge(
        orders_db, left_on="payment_reference", right_on="order_id", how="left"
    )
    
    df_all = pd.concat([df_non_januar, df_januar], ignore_index=True)
    df_all["delta"] = np.abs(df_all["order_total"] - df_all["amount"])
    
    mismatches = df_all[
        (df_all.delta >= delta_threshold) & 
        df_all.order_total.notna()
    ].copy()
    
    mismatches = mismatches.sort_values(['delta', 'psp']).reset_index(drop=True)
    
    return mismatches