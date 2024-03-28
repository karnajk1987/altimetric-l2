from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

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

def run_databricks_notebook():
    from airflow.contrib.hooks.databricks_hook import DatabricksHook

    # Initialize DatabricksHook
    databricks_hook = DatabricksHook(databricks_conn_id='your_databricks_connection_id')

    # Specify notebook parameters
    notebook_params = {
        'data_path': Variable.get("data_path")  # Fetching data path from Airflow Variables
    }

    # Submit run to Databricks
    run_id = databricks_hook.submit_run(
        new_cluster={
            'spark_version': '7.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 2,
        },
        notebook_task={
            'notebook_path': '/home/ec2-user/environment/65fd04910420750aa248497b-001/3.workflow_automation/3.1databricks.py',
            'base_parameters': notebook_params
        }
    )
    print("Submitted Databricks run with run_id:", run_id)

# Define Databricks task
databricks_task = PythonOperator(
    task_id='run_databricks_notebook',
    python_callable=run_databricks_notebook,
    dag=dag
)

databricks_task
