import os
import pandas as pd
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_source")

def clean_and_parse_incremental_dates(df: pd.DataFrame, date_cols: list, date_format: str) -> pd.DataFrame:
    """
    Parses date columns for an incremental batch slice using a strict format string.
    """
    df = df.copy()
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format)
    return df

def validate_date_bounds(df: pd.DataFrame, date_column: str, start_date: str, end_date: str) -> bool:
    """
    Asserts whether all dates in a column fall within a specified range.
    """
    min_bound = pd.to_datetime(start_date)
    max_bound = pd.to_datetime(end_date)
    
    actual_min = df[date_column].min()
    actual_max = df[date_column].max()
    
    if actual_min < min_bound or actual_max > max_bound:
        raise ValueError(
            f"Data bounds violation in {date_column}. "
            f"Expected [{start_date} to {end_date}], found [{actual_min} to {actual_max}]."
        )
    return True

def process_orders_and_inventory_dates():
    """
    Executes explicit date cleaning pipelines on orders and inventory files.
    """
    orders_path = os.path.join(RAW_DIR, "qcommerce_orders.csv")
    inventory_path = os.path.join(RAW_DIR, "qcommerce_inventory.csv")
    
    # 1. Process Orders Data
    if os.path.exists(orders_path):
        orders_df = pd.read_csv(orders_path)
        orders_df = clean_and_parse_dates(orders_df, "order_date", "%Y-%m-%d %H:%M:%S")
        orders_df = clean_and_parse_dates(orders_df, "promised_delivery_time", "%Y-%m-%d %H:%M:%S")
        orders_df = clean_and_parse_dates(orders_df, "actual_delivery_time", "%Y-%m-%d %H:%M:%S")
        
        validate_date_bounds(orders_df, "order_date", "2023-03-01", "2024-11-30")
        print(f"Orders date validation complete. Range: {orders_df['order_date'].min()} to {orders_df['order_date'].max()}")
        
    # 2. Process Inventory Data (DD-MM-YYYY format constraint enforced)
    if os.path.exists(inventory_path):
        inv_df = pd.read_csv(inventory_path)
        inv_df = clean_and_parse_dates(inv_df, "date", "%d-%m-%Y")
        
        validate_date_bounds(inv_df, "date", "2023-03-01", "2024-11-30")
        print(f"Inventory date validation complete. Range: {inv_df['date'].min()} to {inv_df['date'].max()}")

if __name__ == "__main__":
    process_orders_and_inventory_dates()