from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from training_inference_pipeline.utils import train_model, tune_model


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="model_training_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

train_task = PythonOperator(
    task_id="train_model",
    python_callable=train_model,
    dag=dag,
)

tune_task = PythonOperator(
    task_id="tune_model",
    python_callable=tune_model,
    dag=dag,
)

train_task.set_downstream(tune_task)
