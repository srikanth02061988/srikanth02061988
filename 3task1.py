from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from azure.cosmos import CosmosClient
import paramiko

# Replace placeholders with your actual values
sftp_host = "<your-sftp-host>"
sftp_port = 22
sftp_username = "<your-sftp-username>"
sftp_password = "<your-sftp-password>"
sftp_remote_path = "/path/to/files"

cosmos_db_endpoint = "<your-cosmos-db-endpoint>"
cosmos_db_key = "<your-cosmos-db-key>"
cosmos_db_database_name = "<your-database-name>"
cosmos_db_container_name = "<your-container-name>"

# Function to extract SFTP information
def extract_sftp_info(**kwargs):
    transport = paramiko.Transport((sftp_host, sftp_port))
    transport.connect(username=sftp_username, password=sftp_password)
    sftp = paramiko.SFTPClient.from_transport(transport)

    files_info = []
    for file in sftp.listdir(sftp_remote_path):
        file_path = f"{sftp_remote_path}/{file}"
        file_stat = sftp.stat(file_path)
        files_info.append({
            "file_name": file,
            "file_path": file_path,
            "file_size": file_stat.st_size,
            "last_modified": datetime.fromtimestamp(file_stat.st_mtime)
        })

    sftp.close()
    transport.close()

    return files_info

# Function to store data in Cosmos DB
def store_in_cosmos_db(**kwargs):
    ti = kwargs['ti']
    files_info = ti.xcom_pull(task_ids='extract_sftp_info')

    client = CosmosClient(cosmos_db_endpoint, cosmos_db_key)
    database = client.get_database_client(cosmos_db_database_name)
    container = database.get_container_client(cosmos_db_container_name)

    for file_info in files_info:
        container.upsert_item(file_info)

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'sftp_to_cosmosdb',
    default_args=default_args,
    description='Transfer data from SFTP to Cosmos DB',
    schedule_interval='@daily',
)

# Define tasks
extract_sftp_task = PythonOperator(
    task_id='extract_sftp_info',
    python_callable=extract_sftp_info,
    provide_context=True,
    dag=dag,
)

store_in_cosmos_db_task = PythonOperator(
    task_id='store_in_cosmosdb',
    python_callable=store_in_cosmos_db,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_sftp_task >> store_in_cosmos_db_task