from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from training_inference_pipeline.constants import *
from training_inference_pipeline.utils import (
    run_inference,
    monitor_drift,
    train_model,
    tune_model,
    send_email_notification,
    read_drift_score,
)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def decide_action():
    """Decide what action to take based on the drift score."""
    drift_score = read_drift_score()
    if drift_score > PRIMARY_DRIFT_THRESHOLD:
        return "retrain_model"
    elif drift_score > SECONDARY_DRIFT_THRESHOLD:
        return "tune_model"
    elif drift_score > TERTIARY_DRIFT_THRESHOLD:
        return "send_email_notification"
    else:
        return "no_action"


dag = DAG(
    dag_id="inference_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

inference_task = PythonOperator(
    task_id="run_inference", python_callable=run_inference, dag=dag
)

monitor_drift_task = PythonOperator(
    task_id="monitor_drift", python_callable=monitor_drift, dag=dag
)

decide_action_task = BranchPythonOperator(
    task_id="decide_action", python_callable=decide_action, dag=dag
)

retrain_task = PythonOperator(task_id="retrain_model", python_callable=train_model, dag=dag)

tune_task = PythonOperator(task_id="tune_model", python_callable=tune_model, dag=dag)

email_task = PythonOperator(
    task_id="send_email_notification", python_callable=send_email_notification, dag=dag
)

no_action_task = PythonOperator(
    task_id="no_action", python_callable=lambda: print("No action required."), dag=dag
)

# Task dependencies
inference_task >> monitor_drift_task >> decide_action_task
decide_action_task >> [retrain_task, tune_task, email_task, no_action_task]
