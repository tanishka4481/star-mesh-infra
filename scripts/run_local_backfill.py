import os
import sys
import pandas as pd

# Map execution paths back to your active logic scripts
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.cleaning import clean_and_parse_incremental_dates
from scripts.load_staging import load_dataframe_to_staging
from scripts.build_facts import append_incremental_order_fact
from scripts.compute_metrics import calculate_daily_slice_metrics
from scripts.check_thresholds import evaluate_metrics_against_thresholds
from scripts.send_alert import dispatch_anomaly_alert

REPLAY_BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "daily_replay")

def execute_full_database_backfill():
    """
    Loops chronologically through all daily replay data chunks, running the validation,
    staging load, and fact schema append tasks directly into Postgres.
    """
    if not os.path.exists(REPLAY_BASE_DIR):
        print(f"[!] Replay source directory missing at: {REPLAY_BASE_DIR}")
        return
        
    # Isolate and sort partition folders to guarantee chronological day-by-day insertion
    partitions = sorted([d for d in os.listdir(REPLAY_BASE_DIR) if d.startswith("date_partition_")])
    
    print(f"[*] Found {len(partitions)} daily data slices. Starting database backfill...")
    
    for partition_name in partitions:
        target_date_str = partition_name.replace("date_partition_", "")
        partition_path = os.path.join(REPLAY_BASE_DIR, partition_name)
        
        orders_file = os.path.join(partition_path, "orders.csv")
        items_file = os.path.join(partition_path, "order_items.csv")
        
        if not os.path.exists(orders_file) or not os.path.exists(items_file):
            continue
            
        print(f"⚡ Processing date slice: {target_date_str}")
        
        # 1. Extract & Raw Validate
        orders_df = pd.read_csv(orders_file)
        items_df = pd.read_csv(items_file)
        
        # 2. Clean & Transform Dates
        orders_df = clean_and_parse_incremental_dates(
            orders_df, ["order_date", "promised_delivery_time", "actual_delivery_time"], "%Y-%m-%d %H:%M:%S"
        )
        
        # 3. Load to Postgres Staging Tables
        load_dataframe_to_staging(orders_df, "stg_orders", if_exists_rule="append")
        load_dataframe_to_staging(items_df, "stg_order_items", if_exists_rule="append")
        
        # 4. Transform and Stream to Postgres Production Fact Layer
        # This function appends to local csv backup AND appends directly to Postgres 'fact_order_line'
        append_incremental_order_fact(orders_df, items_df)
        
        # 5. Compute Metrics & Check Threshold Anomaly Alerts
        # (This keeps our alerts system active during execution processing)
        orders_df["order_id"] = orders_df["order_id"].astype(int)
        items_df["order_id"] = items_df["order_id"].astype(int)
        day_slice = pd.merge(items_df, orders_df, on="order_id", how="inner")
        day_slice["is_late"] = (day_slice["delivery_status"] != "On Time").astype(int)
        
        daily_metrics = calculate_daily_slice_metrics(day_slice)
        detected_breaches = evaluate_metrics_against_thresholds(daily_metrics)
        
        if detected_breaches:
            print(f"   ⚠️ Alert triggered for {target_date_str}! Sending notification report...")
            dispatch_anomaly_alert(detected_breaches, target_date_str)

    print("\n🎉 Database backfill completely finished! All data successfully modeled inside Postgres.")

if __name__ == "__main__":
    execute_full_database_backfill()