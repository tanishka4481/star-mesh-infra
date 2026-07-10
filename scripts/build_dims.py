import os
import pandas as pd


RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw_source")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed_dimensions")

def generate_dim_date(start_date: str = "2023-03-01", end_date: str = "2024-11-30") -> pd.DataFrame:
    """
    Generates a deterministic time-dimension calendar table for BI tracking.
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_date = pd.DataFrame({
        "date_key": date_range.strftime("%Y%m%d").astype(int),
        "full_date": date_range,
        "year": date_range.year,
        "month": date_range.month,
        "day": date_range.day,
        "day_of_week": date_range.dayofweek + 1,  # ISO Standard: 1 = Monday, 7 = Sunday
        "day_name": date_range.strftime("%A"),
        "month_name": date_range.strftime("%B"),
        "is_weekend": date_range.dayofweek.isin([5, 6]).astype(int)
    })
    return dim_date

def build_entity_dimensions():
    """
    Extracts and standardizes core structural descriptive dimensions.
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Build dim_date
    dim_date_df = generate_dim_date()
    dim_date_df.to_csv(os.path.join(OUTPUT_DIR, "dim_date.csv"), index=False)
    print(f"Generated dim_date.csv with {len(dim_date_df)} rows.")

    # 2. Build dim_customer
    cust_path = os.path.join(RAW_DIR, "qcommerce_customers.csv")
    if os.path.exists(cust_path):
        cust_df = pd.read_csv(cust_path)
        dim_customer = pd.DataFrame({
            "customer_id": cust_df["customer_id"].astype(int),
            "customer_name": cust_df["customer_name"].fillna("Unknown").astype(str),
            "email": cust_df["email"].fillna("Unknown").astype(str),
            "phone": cust_df["phone"].fillna(0).astype(int),
            "customer_segment": cust_df["customer_segment"].fillna("Standard").astype(str)
        })
        dim_customer.to_csv(os.path.join(OUTPUT_DIR, "dim_customer.csv"), index=False)
        print(f"Generated dim_customer.csv with {len(dim_customer)} rows.")

    # 3. Build dim_product
    prod_path = os.path.join(RAW_DIR, "qcommerce_products.csv")
    if os.path.exists(prod_path):
        prod_df = pd.read_csv(prod_path)
        dim_product = pd.DataFrame({
            "product_id": prod_df["product_id"].astype(int),
            "product_name": prod_df["product_name"].fillna("Unknown").astype(str),
            "category": prod_df["category"].fillna("Unknown").astype(str),
            "brand": prod_df["brand"].fillna("Unknown").astype(str),
            "price": prod_df["price"].fillna(0.0).astype(float),
            "mrp": prod_df["mrp"].fillna(0.0).astype(float),
            "margin_percentage": prod_df["margin_percentage"].fillna(0.0).astype(float),
            "shelf_life_days": prod_df["shelf_life_days"].fillna(0).astype(int)
        })
        dim_product.to_csv(os.path.join(OUTPUT_DIR, "dim_product.csv"), index=False)
        print(f"Generated dim_product.csv with {len(dim_product)} rows.")

    # 4. Build dim_store (Derived defensively from master transactional logs)
    orders_path = os.path.join(RAW_DIR, "qcommerce_orders.csv")
    if os.path.exists(orders_path):
        orders_df = pd.read_csv(orders_path)
        
        unique_stores = orders_df["store_id"].dropna().unique()
        dim_store = pd.DataFrame({
            "store_id": unique_stores.astype(int),
            "store_name": [f"Fulfillment Center Hub {int(sid)}" for sid in unique_stores]
        })
        dim_store.to_csv(os.path.join(OUTPUT_DIR, "dim_store.csv"), index=False)
        print(f"Generated dim_store.csv with {len(dim_store)} unique nodes.")

        # 5. Build dim_delivery_partner
        unique_partners = orders_df["delivery_partner_id"].dropna().unique()
        dim_delivery_partner = pd.DataFrame({
            "delivery_partner_id": unique_partners.astype(int),
            "partner_name": [f"Fleet Agent {int(pid)}" for pid in unique_partners]
        })
        dim_delivery_partner.to_csv(os.path.join(OUTPUT_DIR, "dim_delivery_partner.csv"), index=False)
        print(f"Unique stores: {orders_df['store_id'].nunique()}")
        print(f"Unique delivery partners: {orders_df['delivery_partner_id'].nunique()}")
        print(f"Delivery status counts:\n{orders_df['delivery_status'].value_counts()}")
        print(f"Generated dim_delivery_partner.csv with {len(dim_delivery_partner)} unique agents.")

if __name__ == "__main__":
    build_entity_dimensions()