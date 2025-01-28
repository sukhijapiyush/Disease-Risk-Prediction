# Paths
TRAIN_DATA_PATH = "/opt/airflow/dags/training_inference_pipeline/data/Train.csv"
REFERENCE_DATA_PATH = "/opt/airflow/dags/training_inference_pipeline/data/Train.csv"
INFERENCE_DATA_PATH = "/opt/airflow/dags/training_inference_pipeline/data/Test.csv"
MODEL_PATH = "/opt/airflow/dags/training_inference_pipeline/models/xgboost"
DRIFT_REPORT_PATH = "/opt/airflow/dags/training_inference_pipeline/models/drift_report.html"

# Drift thresholds
PRIMARY_DRIFT_THRESHOLD = 0.7
SECONDARY_DRIFT_THRESHOLD = 0.5
TERTIARY_DRIFT_THRESHOLD = 0.3
