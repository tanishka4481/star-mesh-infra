import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.cleaning import clean_and_parse_incremental_dates
from scripts.build_facts import append_incremental_order_fact
from scripts.compute_metrics import calculate_daily_slice_metrics
from scripts.check_thresholds import evaluate_metrics_against_thresholds
from scripts.send_alert import dispatch_anomaly_alert
from scripts.load_staging import load_dataframe_to_staging

REPLAY_BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "daily_replay")
PROCESSED_FACTS_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed_facts")

default_args = {
    "owner": "data_engineering_platform",
    "depends_on_past": True,
    "start_date": datetime(2024, 10, 1), 
    "end_date": datetime(2024, 11, 4),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    "qcommerce_incremental_production_pipeline",
    default_args=default_args,
    description="True incremental quick-commerce ingestion, validation, star-schema build, and SLA auditing.",
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True
)

# ==========================================
# PRODUCTION TASK EXECUTABLE LOGIC
# ==========================================

def _extract_daily_slice(**context):
    """Reads execution date token and loads paths for data verification."""
    ds = context["ds"]
    partition_path = os.path.join(REPLAY_BASE_DIR, f"date_partition_{ds}")
    
    if not os.path.exists(partition_path):
        raise FileNotFoundError(f"Extraction halted: Missing data slice folder for partition: {partition_path}")
        
    context["ti"].xcom_push(key="active_partition_path", value=partition_path)

def _validate_raw(**context):
    """Verifies physical file schemas and structural integrity of the partition."""
    partition_path = context["ti"].xcom_pull(task_ids="extract_daily_slice", key="active_partition_path")
    orders_file = os.path.join(partition_path, "orders.csv")
    items_file = os.path.join(partition_path, "order_items.csv")
    
    # Enforce data quality assertion checks
    for f in [orders_file, items_file]:
        if not os.path.exists(f) or os.path.getsize(f) == 0:
            raise ValueError(f"Data Quality Breach: Target slice corrupted or empty at tracking node: {f}")
            
    orders_df = pd.read_csv(orders_file)
    expected_cols = ["order_id", "customer_id", "order_date", "order_total", "delivery_status"]
    for col in expected_cols:
        if col not in orders_df.columns:
            raise KeyError(f"Schema Validation Failure: Missing required column structural constraint -> {col}")

def _clean_transform_incremental(**context):
    """Executes target formatting over dates inside the active execution slice."""
    partition_path = context["ti"].xcom_pull(task_ids="extract_daily_slice", key="active_partition_path")
    
    orders_df = pd.read_csv(os.path.join(partition_path, "orders.csv"))
    items_df = pd.read_csv(os.path.join(partition_path, "order_items.csv"))
    
    # Enforce standard formatting to resolve downstream timestamp differences
    orders_df = clean_and_parse_incremental_dates(
        orders_df, ["order_date", "promised_delivery_time", "actual_delivery_time"], "%Y-%m-%d %H:%M:%S"
    )
    
    # Stash clean payloads into XCom storage to avoid redundant disk lookups
    context["ti"].xcom_push(key="clean_orders_df_json", value=orders_df.to_json(date_format='iso'))
    context["ti"].xcom_push(key="clean_items_df_json", value=items_df.to_json())

# (Locate and replace the old dummy _load_staging task wrapper function with this live implementation)
def _load_staging(**context):
    """
    Streams the current active day's sanitized staging records into the live database infrastructure.
    """
    ti = context["ti"]
    orders_json = ti.xcom_pull(task_ids="clean_transform", key="clean_orders_df_json")
    items_json = ti.xcom_pull(task_ids="clean_transform", key="clean_items_df_json")
    
    orders_df = pd.read_json(orders_json)
    items_df = pd.read_json(items_json)
    
    # Enforce strict date parsing patterns over json deserialization strings
    for col in ["order_date", "promised_delivery_time", "actual_delivery_time"]:
        if col in orders_df.columns:
            orders_df[col] = pd.to_datetime(orders_df[col])
            
    # Load separate operational segments into isolated database landing zones
    load_dataframe_to_staging(orders_df, "stg_orders", if_exists_rule="append")
    load_dataframe_to_staging(items_df, "stg_order_items", if_exists_rule="append")
    
    print(f"Successfully streamed staging batch records into relational target system for date: {context['ds']}")

def _build_star_schema_incremental(**context):
    """Transforms clean active payloads and safely appends them into fact files."""
    ti = context["ti"]
    orders_json = ti.xcom_pull(task_ids="clean_transform", key="clean_orders_df_json")
    items_json = ti.xcom_pull(task_ids="clean_transform", key="clean_items_df_json")
    
    orders_df = pd.read_json(orders_json)
    items_df = pd.read_json(items_json)
    
    # Convert text strings back to real datetime parameters
    for col in ["order_date", "promised_delivery_time", "actual_delivery_time"]:
        if col in orders_df.columns:
            orders_df[col] = pd.to_datetime(orders_df[col])
            
    # Process transformation logic and append straight into the production fact table
    append_incremental_order_fact(orders_df, items_df)

def _compute_daily_metrics_incremental(**context):
    """Computes exact analytics matrices over the active processing cycle slice."""
    ti = context["ti"]
    orders_json = ti.xcom_pull(task_ids="clean_transform", key="clean_orders_df_json")
    items_json = ti.xcom_pull(task_ids="clean_transform", key="clean_items_df_json")
    
    orders_df = pd.read_json(orders_json)
    items_df = pd.read_json(items_json)
    
    # Compute active metrics metrics utilizing our functional package modules
    orders_df["order_id"] = orders_df["order_id"].astype(int)
    items_df["order_id"] = items_df["order_id"].astype(int)
    
    day_slice = pd.merge(items_df, orders_df, on="order_id", how="inner")
    day_slice["is_late"] = (day_slice["delivery_status"] != "On Time").astype(int)
    
    daily_metrics = calculate_daily_slice_metrics(day_slice)
    ti.xcom_push(key="daily_metrics_payload", value=daily_metrics)

def _check_thresholds_incremental(**context):
    """Evaluates computed daily summaries against our python config properties file."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="compute_daily_metrics", key="daily_metrics_payload")
    
    detected_breaches = evaluate_metrics_against_thresholds(metrics)
    ti.xcom_push(key="detected_anomalies_list", value=detected_breaches)

def _send_alert_incremental(**context):
    """Triggers smtp pipeline alert communication hooks if anomalies are caught."""
    ds = context["ds"]
    breaches = context["ti"].xcom_pull(task_ids="check_thresholds", key="detected_anomalies_list")
    
    if breaches:
        dispatch_anomaly_alert(breaches, ds)
    else:
        print(f"Operational performance target indicators completely within limits for date: {ds}")

# ==========================================
# TASK ORCHESTRATION LAYER DEFINITIONS
# ==========================================

task_extract = PythonOperator(task_id="extract_daily_slice", python_callable=_extract_daily_slice, dag=dag)
task_validate = PythonOperator(task_id="validate_raw", python_callable=_validate_raw, dag=dag)
task_clean    = PythonOperator(task_id="clean_transform", python_callable=_clean_transform_incremental, dag=dag)
task_load     = PythonOperator(task_id="load_staging", python_callable=_load_staging, dag=dag)
task_model    = PythonOperator(task_id="build_star_schema", python_callable=_build_star_schema_incremental, dag=dag)
task_metrics  = PythonOperator(task_id="compute_daily_metrics", python_callable=_compute_daily_metrics_incremental, dag=dag)
task_check    = PythonOperator(task_id="check_thresholds", python_callable=_check_thresholds_incremental, dag=dag)
task_alert    = PythonOperator(task_id="send_alert", python_callable=_send_alert_incremental, dag=dag)

task_extract >> task_validate >> task_clean >> task_load >> task_model >> task_metrics >> task_check >> task_alert