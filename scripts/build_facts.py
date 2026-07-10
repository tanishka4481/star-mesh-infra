import os
import pandas as pd

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_source")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed_facts")

def load_and_parse_raw_timestamps(file_path: str, date_cols: list, date_format: str = "%Y-%m-%d %H:%M:%S") -> pd.DataFrame:
    """
    Reads raw source files and applies specified format masks to remove parsing drift.
    """
    df = pd.read_csv(file_path)
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format=date_format)
    return df

# (Inside scripts/build_facts.py, ensure this import sits at the top)
from scripts.load_staging import load_dataframe_to_staging

def append_incremental_order_fact(orders_df: pd.DataFrame, items_df: pd.DataFrame):
    """
    Transforms and appends a single day's increment into fact_order_line 
    with a clean integer surrogate date key to fix Power BI joins.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    fact_path = os.path.join(OUTPUT_DIR, "fact_order_line.csv")
    
    items_df["order_id"] = items_df["order_id"].astype(int)
    items_df["product_id"] = items_df["product_id"].astype(int)
    orders_df["order_id"] = orders_df["order_id"].astype(int)
    
    increment_df = pd.merge(items_df, orders_df, on="order_id", how="inner")
    
    # Ensure order_date is completely read as a real datetime object first
    increment_df["order_date"] = pd.to_datetime(increment_df["order_date"])
    
    # 🚨 THE CRITICAL FIX: Convert datetime string to pure YYYYMMDD integer key
    increment_df["order_date_key"] = increment_df["order_date"].dt.strftime("%Y%m%d").astype(int)
    
    delivery_delta = increment_df["actual_delivery_time"] - increment_df["promised_delivery_time"]
    increment_df["delivery_delay_minutes"] = delivery_delta.dt.total_seconds() / 60.0
    increment_df["is_late"] = (increment_df["delivery_status"] != "On Time").astype(int)
    
    # Append the new 'order_date_key' explicitly to your selected target fields
    final_fields = [
        "order_id", "customer_id", "product_id", "store_id", "delivery_partner_id",
        "order_date", "order_date_key", "quantity", "unit_price", "order_total", 
        "delivery_status", "is_late", "payment_method", "delivery_delay_minutes"
    ]
    increment_df = increment_df[final_fields]
    
    # 1. Update cold storage flat file
    if not os.path.exists(fact_path):
        increment_df.to_csv(fact_path, index=False)
    else:
        increment_df.to_csv(fact_path, mode='a', header=False, index=False)
        
    # 2. Stream to Postgres (Using replace here temporarily to wipe out the old broken schema table)
    load_dataframe_to_staging(increment_df, "fact_order_line", if_exists_rule="append")
       
def build_star_schema_facts():
    """
    Constructs the operational fact engine layers.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    orders_path = os.path.join(RAW_DIR, "qcommerce_orders.csv")
    items_path = os.path.join(RAW_DIR, "qcommerce_order_items.csv")
    marketing_path = os.path.join(RAW_DIR, "qcommerce_marketing.csv")
    feedback_path = os.path.join(RAW_DIR, "qcommerce_feedback.csv")
    inventory_path = os.path.join(RAW_DIR, "qcommerce_inventory.csv")

    # ==========================================
    # fact_order_line ENGINE BUILD
    # ==========================================
    if os.path.exists(orders_path) and os.path.exists(items_path):
        orders_df = load_and_parse_raw_timestamps(
            orders_path, ["order_date", "promised_delivery_time", "actual_delivery_time"]
        )
        items_df = pd.read_csv(items_path)
        
        # Enforce exact integer constraints to drop scientific float artifacts
        items_df["order_id"] = items_df["order_id"].astype(int)
        items_df["product_id"] = items_df["product_id"].astype(int)
        orders_df["order_id"] = orders_df["order_id"].astype(int)
        
        fact_order_line = pd.merge(items_df, orders_df, on="order_id", how="inner")
        
        # Continuous variable duration computation (Signed Minutes)
        delivery_delta = fact_order_line["actual_delivery_time"] - fact_order_line["promised_delivery_time"]
        fact_order_line["delivery_delay_minutes"] = delivery_delta.dt.total_seconds() / 60.0
        
        # Binary flag logic derived explicitly from corporate grace-period rule boundaries
        fact_order_line["is_late"] = (fact_order_line["delivery_status"] != "On Time").astype(int)
        
        fact_order_line = fact_order_line[[
            "order_id", "customer_id", "product_id", "store_id", "delivery_partner_id",
            "order_date", "quantity", "unit_price", "order_total", 
            "delivery_status", "is_late", "payment_method", "delivery_delay_minutes"
        ]]
        
        fact_order_line.to_csv(os.path.join(OUTPUT_DIR, "fact_order_line.csv"), index=False)
        print(f"Generated fact_order_line.csv with {len(fact_order_line)} rows.")

    # ==========================================
    # fact_marketing ENGINE BUILD
    # ==========================================
    if os.path.exists(marketing_path):
        mkt_df = load_and_parse_raw_timestamps(marketing_path, ["date"], "%Y-%m-%d")
        
        fact_marketing = pd.DataFrame({
            "campaign_id": mkt_df["campaign_id"].astype(int),
            "date": mkt_df["date"],
            "impressions": mkt_df["impressions"].fillna(0).astype(int),
            "clicks": mkt_df["clicks"].fillna(0).astype(int),
            "conversions": mkt_df["conversions"].fillna(0).astype(int),
            "spend": mkt_df["spend"].fillna(0.0).astype(float),
            "revenue_generated": mkt_df["revenue_generated"].fillna(0.0).astype(float)
        })
        
        fact_marketing.to_csv(os.path.join(OUTPUT_DIR, "fact_marketing.csv"), index=False)
        print(f"Generated fact_marketing.csv with {len(fact_marketing)} rows.")

    # ==========================================
    # fact_feedback ENGINE BUILD
    # ==========================================
    if os.path.exists(feedback_path):
        fb_df = load_and_parse_raw_timestamps(feedback_path, ["feedback_date"], "%Y-%m-%d")
        
        fact_feedback = pd.DataFrame({
            "feedback_id": fb_df["feedback_id"].astype(int),
            "order_id": fb_df["order_id"].astype(int),
            "customer_id": fb_df["customer_id"].astype(int),
            "date": fb_df["feedback_date"],
            "rating": fb_df["rating"].fillna(0).astype(int),
            "sentiment": fb_df["sentiment"].fillna("Unknown").astype(str)
        })
        
        fact_feedback.to_csv(os.path.join(OUTPUT_DIR, "fact_feedback.csv"), index=False)
        print(f"Generated fact_feedback.csv with {len(fact_feedback)} rows.")

    # ==========================================
    # fact_inventory ENGINE BUILD
    # ==========================================
    if os.path.exists(inventory_path):
        inv_df = load_and_parse_raw_timestamps(inventory_path, ["date"], "%d-%m-%Y")
        
        fact_inventory = pd.DataFrame({
            "product_id": inv_df["product_id"].astype(int),
            "date": inv_df["date"],
            "stock_received": inv_df["stock_received"].fillna(0).astype(int),
            "damaged_stock": inv_df["damaged_stock"].fillna(0).astype(int)
        })
        
        fact_inventory.to_csv(os.path.join(OUTPUT_DIR, "fact_inventory.csv"), index=False)
        print(f"Generated fact_inventory.csv with {len(fact_inventory)} rows.")

if __name__ == "__main__":
    build_star_schema_facts()