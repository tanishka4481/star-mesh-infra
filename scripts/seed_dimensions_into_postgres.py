import os
import sys
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.load_staging import load_dataframe_to_staging

DIM_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed_dimensions")

def seed_all_dimensions():
    """
    Reads static dimensions from disk and registers them once into Postgres.
    """
    dimensions_map = {
        "dim_date.csv": "dim_date",
        "dim_customer.csv": "dim_customer",
        "dim_product.csv": "dim_product",
        "dim_store.csv": "dim_store",
        "dim_delivery_partner.csv": "dim_delivery_partner"
    }
    
    print("Initializing dimension database seed sequence...")
    
    for filename, table_name in dimensions_map.items():
        file_path = os.path.join(DIM_DIR, filename)
        if not os.path.exists(file_path):
            print(f"[!] Warning: Dimension file missing from disk: {filename}. Run build_dims.py first.")
            continue
            
        df = pd.read_csv(file_path)
        
        # Overwrite on initial seeding to guarantee schema structures are fresh
        load_dataframe_to_staging(df, table_name, if_exists_rule="replace")
        print(f"  • Successfully seeded table [{table_name}] with {len(df)} records.")

if __name__ == "__main__":
    seed_all_dimensions()