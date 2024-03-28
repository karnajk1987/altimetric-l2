from airflow import DAG
from airflow.operators.databricks_operator import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'data_processing_workflow',
    default_args=default_args,
    description='A daily workflow for data processing',
    schedule_interval=timedelta(days=1),
)

# Define Databricks task
databricks_task = DatabricksSubmitRunOperator(
    task_id='run_databricks_notebook',
    dag=dag,
    new_cluster={
        'spark_version': '7.3.x-scala2.12',
        'node_type_id': 'i3.xlarge',
        'num_workers': 2,
    },
    notebook_task={
        'notebook_path': '/path/to/your/notebook/data_processing_notebook',
        'base_parameters': {
            'data_path': 's3://your-bucket/data.csv'  # Specify the path to your dataset file
        }
    },
)

