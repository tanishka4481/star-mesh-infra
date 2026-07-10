import pandas as pd

def calculate_daily_slice_metrics(orders_slice_df: pd.DataFrame) -> dict:
    """
    Computes unique order counts, total net revenue, and the SLA failure rate
    for a single day's input dataframe.
    """
    if orders_slice_df.empty:
        return {
            "order_count": 0,
            "total_revenue": 0.0,
            "sla_failure_pct": 0.0
        }
    
    # 1. Compute granular volume and revenue metrics
    unique_orders = int(orders_slice_df["order_id"].nunique())
    total_revenue = float(orders_slice_df["order_total"].sum())
    
    # 2. Compute true SLA breaches using the business-aligned is_late flag
    total_late_orders = int(orders_slice_df["is_late"].sum())
    sla_failure_pct = (total_late_orders / unique_orders) * 100.0 if unique_orders > 0 else 0.0
    
    return {
        "order_count": unique_orders,
        "total_revenue": round(total_revenue, 2),
        "sla_failure_pct": round(sla_failure_pct, 2)
    }

if __name__ == "__main__":
    # Self-test using a dummy data framework to maintain stand-alone testability
    test_df = pd.DataFrame({
        "order_id": [1, 2, 3, 4],
        "order_total": [150.0, 200.0, 50.0, 300.0],
        "is_late": [0, 1, 0, 1]
    })
    test_results = calculate_daily_slice_metrics(test_df)
    print(f"Self-Test Metrics Output: {test_results}")