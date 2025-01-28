import pandas as pd
from pycaret.classification import (
    setup,
    compare_models,
    finalize_model,
    save_model,
    load_model,
    predict_model,
)
from evidently.report import Report
from evidently.metrics import DataDriftTable
import smtplib
import os
from training_inference_pipeline.constants import *


def train_model():
    """Train a model using PyCaret and save it."""
    train_data = pd.read_csv(TRAIN_DATA_PATH)
    s = setup(data=train_data, target="target", silent=True, session_id=123)
    best_model = compare_models()
    finalized_model = finalize_model(best_model)
    save_model(finalized_model, MODEL_PATH)
    print("Model trained and saved.")


def tune_model():
    """Hyperparameter tune a model using PyCaret."""
    train_data = pd.read_csv(TRAIN_DATA_PATH)
    s = setup(data=train_data, target="target", silent=True, session_id=123)
    best_model = compare_models(turbo=False)
    finalized_model = finalize_model(best_model)
    save_model(finalized_model, MODEL_PATH)
    print("Model tuned and saved.")


def run_inference():
    """Run inference using the saved PyCaret model."""
    inference_data = pd.read_csv(INFERENCE_DATA_PATH)
    model = load_model(MODEL_PATH)
    predictions = predict_model(model, data=inference_data)
    predictions.to_csv("/opt/airflow/dags/training_inference_pipeline/data/inference_results.csv", index=False)
    print("Inference completed.")


def monitor_drift():
    """Monitor data drift using Evidently and save the report."""
    reference_data = pd.read_csv(REFERENCE_DATA_PATH)
    inference_data = pd.read_csv(INFERENCE_DATA_PATH)
    drift_report = Report(metrics=[DataDriftTable()])
    drift_report.run(reference_data=reference_data, current_data=inference_data)
    drift_report.save_html(DRIFT_REPORT_PATH)

    # Extract drift score
    drift_results = drift_report.as_dict()
    print(drift_results)
    is_drift = drift_results["metrics"][0]["result"]["dataset_drift"]
    if is_drift==False:
        drift_score = 0.0
    else:
        number_of_drifted_columns = drift_results["metrics"][0]["result"]["number_of_drifted_columns"]
        number_of_columns = drift_results["metrics"][0]["result"]["number_of_columns"]
        drift_score = number_of_drifted_columns / number_of_columns
    with open("/opt/airflow/dags/training_inference_pipeline/models/drift_score.txt", "w") as f:
        f.write(str(drift_score))
    print(f"Drift score: {drift_score}")
    return drift_score


def read_drift_score():
    """Read the stored drift score from file."""
    if os.path.exists("models/drift_score.txt"):
        with open("/opt/airflow/dags/training_inference_pipeline/models/drift_score.txt", "r") as f:
            return float(f.read().strip())
    return 0.0


def send_email_notification():
    """Send an email notification for drift."""
    recipient = "admin@example.com"
    subject = "Data Drift Alert"
    body = "Drift detected in the model. Please take necessary actions."
    sender_email = "your-email@example.com"
    sender_password = "your-email-password"

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        message = f"Subject: {subject}\n\n{body}"
        server.sendmail(sender_email, recipient, message)
    print("Email notification sent.")
