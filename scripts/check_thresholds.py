import os
import sys

# Compute the definitive pathing anchor locations
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(CURRENT_DIR)
CONFIG_DIR = os.path.join(ROOT_DIR, "config")

# Insert configuration directory directly to resolve standalone script execution pathing
if CONFIG_DIR not in sys.path:
    sys.path.insert(0, CONFIG_DIR)

try:
    import thresholds
except ImportError:
    # Fallback to absolute relative file system loading if module binding is strict
    import importlib.util
    config_spec = importlib.util.spec_from_file_location("thresholds", os.path.join(CONFIG_DIR, "thresholds.py"))
    thresholds = importlib.util.module_from_spec(config_spec)
    config_spec.loader.exec_module(thresholds)

def evaluate_metrics_against_thresholds(metrics: dict) -> list:
    """
    Compares computed daily summary statistics against system-wide thresholds.
    Returns a structured list detailing every individual system breach.
    """
    breaches = []
    
    # 1. Evaluate SLA Failure Rate Anomaly
    if metrics["sla_failure_pct"] > thresholds.SLA_BREACH_THRESHOLD_PCT:
        breaches.append({
            "metric": "SLA Failure Rate",
            "actual_value": f"{metrics['sla_failure_pct']}%",
            "threshold_value": f"{thresholds.SLA_BREACH_THRESHOLD_PCT}%",
            "condition": "Exceeded"
        })
        
    # 2. Evaluate Total Revenue Drop Anomaly
    if metrics["total_revenue"] < thresholds.REVENUE_DROP_THRESHOLD_INR:
        breaches.append({
            "metric": "Daily Revenue",
            "actual_value": f"INR {metrics['total_revenue']:,}",
            "threshold_value": f"INR {thresholds.REVENUE_DROP_THRESHOLD_INR:,}",
            "condition": "Fell Below"
        })
        
    # 3. Evaluate Order Volume Critical Anomaly
    if metrics["order_count"] < thresholds.ORDER_VOLUME_LOW_THRESHOLD:
        breaches.append({
            "metric": "Daily Order Volume",
            "actual_value": str(metrics["order_count"]),
            "threshold_value": str(thresholds.ORDER_VOLUME_LOW_THRESHOLD),
            "condition": "Fell Below"
        })
        
    return breaches

if __name__ == "__main__":
    # Stand-alone pipeline test case simulating a critical failure day
    mock_failed_metrics = {
        "order_count": 2,
        "total_revenue": 450.00,
        "sla_failure_pct": 100.00
    }
    detected_breaches = evaluate_metrics_against_thresholds(mock_failed_metrics)
    print(f"Self-Test Thresholds Audit: Detected {len(detected_breaches)} anomalies.")
    for breach in detected_breaches:
        print(f"  • Alert on [{breach['metric']}]: Actual {breach['actual_value']} {breach['condition']} threshold boundary of {breach['threshold_value']}")