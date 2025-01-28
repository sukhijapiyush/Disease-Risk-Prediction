from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_processing_pipeline.utils import *
from data_processing_pipeline.data_validation_checks import *


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 20),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


data_cleaning_dag = DAG(
    dag_id="Disease_Risk_Prediction",
    default_args=default_args,
    description="DAG to run data pipeline for disease risk prediction",
    schedule="@daily",
    catchup=True,
)

building_db = PythonOperator(
    task_id="building_db", python_callable=build_dbs, dag=data_cleaning_dag
)

checking_raw_data_schema = PythonOperator(
    task_id="checking_raw_data_schema",
    python_callable=raw_data_schema_check,
    dag=data_cleaning_dag,
)

loading_data = PythonOperator(
    task_id="loading_data", python_callable=load_data_into_db, dag=data_cleaning_dag
)

checking_model_inputs_schema = PythonOperator(
    task_id="checking_model_inputs_schema",
    python_callable=model_input_schema_check,
    dag=data_cleaning_dag,
)


building_db.set_downstream(checking_raw_data_schema)
checking_raw_data_schema.set_downstream(loading_data)
loading_data.set_downstream(checking_model_inputs_schema)
