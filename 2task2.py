from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define your Airflow DAG
dag = DAG(
    "databricks_job_creator_dag",
    schedule_interval=None,  # Define your schedule
    start_date=datetime(2023, 10, 30),  # Define your start date
    catchup=False,
)

def create_databricks_job_task():
    # Replace with the path to your script
    script_path = "/path/to/databricks_job_creator.py"
    os.system(f"python {script_path}")

create_databricks_job = PythonOperator(
    task_id="create_databricks_job",
    python_callable=create_databricks_job_task,
    dag=dag,
)

# Add any other tasks or dependencies as needed

if __name__ == "__main__":
    dag.cli()
