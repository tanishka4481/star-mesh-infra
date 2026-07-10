import os
import pandas as pd

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_source")
REPLAY_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "daily_replay")

def segment_dataset_into_daily_stream():
    """
    Slices transaction and logging records chronologically to build out
    the directory partition infrastructure for daily replay automation.
    """
    orders_path = os.path.join(RAW_DIR, "qcommerce_orders.csv")
    items_path = os.path.join(RAW_DIR, "qcommerce_order_items.csv")
    feedback_path = os.path.join(RAW_DIR, "qcommerce_feedback.csv")

    if not (os.path.exists(orders_path) and os.path.exists(items_path) and os.path.exists(feedback_path)):
        print("[!] Critical Error: Required raw transactional source files are missing.")
        return

    # 1. Load data streams and enforce strict key parsing
    orders_df = pd.read_csv(orders_path)
    items_df = pd.read_csv(items_path)
    feedback_df = pd.read_csv(feedback_path)

    orders_df["order_id"] = orders_df["order_id"].astype(int)
    items_df["order_id"] = items_df["order_id"].astype(int)
    feedback_df["order_id"] = feedback_df["order_id"].astype(int)

    # 2. Extract date parameter grain to isolate daily boundaries
    orders_df["parsed_date"] = pd.to_datetime(orders_df["order_date"], format="%Y-%m-%d %H:%M:%S").dt.date
    unique_operational_days = sorted(orders_df["parsed_date"].unique())

    print(f"[*] Processing partitioning logic for {len(unique_operational_days)} historical operational cycles...")

    # 3. Step through chronological days to generate incremental batch drops
    for target_date in unique_operational_days:
        date_str = target_date.strftime("%Y-%m-%d")
        day_dir = os.path.join(REPLAY_DIR, f"date_partition_{date_str}")
        os.makedirs(day_dir, exist_ok=True)

        # Slice Day Orders
        day_orders = orders_df[orders_df["parsed_date"] == target_date].copy()
        day_orders = day_orders.drop(columns=["parsed_date"])
        
        # Pull matching lines for items and customer reviews via relational inner join
        day_order_ids = day_orders["order_id"]
        day_items = items_df[items_df["order_id"].isin(day_order_ids)]
        day_feedback = feedback_df[feedback_df["order_id"].isin(day_order_ids)]

        # Save to disk within target partition signature folder
        day_orders.to_csv(os.path.join(day_dir, "orders.csv"), index=False)
        day_items.to_csv(os.path.join(day_dir, "order_items.csv"), index=False)
        day_feedback.to_csv(os.path.join(day_dir, "feedback.csv"), index=False)

    print(f"Partition processing finished. Output directed to: {REPLAY_DIR}")

if __name__ == "__main__":
    segment_dataset_into_daily_stream()