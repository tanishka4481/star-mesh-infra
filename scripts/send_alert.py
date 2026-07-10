import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Initialize path mapping back to root for secret detection
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

def dispatch_anomaly_alert(breaches: list, operational_date: str) -> bool:
    """
    Constructs and dispatches clean transactional text notification payloads
    over Gmail SMTP transport layer if system anomalies are encountered.
    """
    if not breaches:
        return False

    sender_email = os.getenv("SMTP_EMAIL")
    sender_password = os.getenv("SMTP_PASSWORD")
    receiver_email = os.getenv("ALERT_RECEIVER_EMAIL")

    if not all([sender_email, sender_password, receiver_email]):
        print("[!] Warning: SMTP alert skipped. Missing environment variables in .env.")
        return False

    # 1. Structure message container definitions
    message = MIMEMultipart("alternative")
    message["Subject"] = f"🚨 PIPELINE ALERT: Operational Anomaly Detected [{operational_date}]"
    message["From"] = sender_email
    message["To"] = receiver_email

    # 2. Build scannable report summary body
    body_lines = [
        f"Quick Commerce Operational Performance Alert",
        f"Execution Cycle: {operational_date}",
        "=" * 60,
        "The automated validation engine detected the following threshold breaches:",
        ""
    ]

    for breach in breaches:
        body_lines.append(
            f"• Anomaly Source:   [{breach['metric']}]\n"
            f"  Current Status:   {breach['actual_value']} ({breach['condition']} boundary limit)\n"
            f"  Target Constraint: {breach['threshold_value']}\n"
        )

    body_lines.append("=" * 60)
    body_lines.append("Action Required: Please review the orchestrator partition history.")
    
    text_payload = "\n".join(body_lines)
    message.attach(MIMEText(text_payload, "plain"))

    # 3. Initialize transport and execute wire-delivery
    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        print(f"Operational anomaly alerts successfully dispatched to {receiver_email}.")
        return True
    except Exception as e:
        print(f"[!] Critical SMTP Transmission Failure: {str(e)}")
        return False

if __name__ == "__main__":
    # Test harness execution loop
    mock_breaches = [{
        "metric": "Daily Revenue",
        "actual_value": "INR 450.00",
        "threshold_value": "INR 9,049.13",
        "condition": "Fell Below"
    }]
    dispatch_anomaly_alert(mock_breaches, "2026-07-04")