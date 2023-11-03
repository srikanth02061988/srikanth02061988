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


import subprocess
import json

# Define job configuration
job_name = "MyJobName"
jar_path = "/path/to/your/spark-job.jar"
main_class = "com.example.YourMainClass"
file_path = "/path/to/your/inputfile.txt"
folder_path = "/path/to/your/outputfolder/"
notebook_path = "/Users/your_username/your_notebook"
cluster_id = "cluster_id"

# Define arguments, including both file path and folder path
args = [file_path, folder_path]

job_config = {
    "new_cluster": {
        "spark_version": "7.3.x",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
    },
    "spark_jar_task": {
        "main_class_name": main_class,
        "parameters": args,
        "jar_uri": jar_path,
    },
}

# Create the Databricks job
job_create_command = f"databricks jobs create --name {job_name} --json '{json.dumps(job_config)}'"

try:
    subprocess.run(job_create_command, shell=True, check=True)
    print(f"Job '{job_name}' created successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error creating job: {e}")
