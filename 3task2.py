import paramiko
from datetime import datetime
from azure.cosmos import CosmosClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# SFTP connection details
sftp_host = "<your-sftp-host>"
sftp_port = 22
sftp_username = "<your-sftp-username>"
sftp_password = "<your-sftp-password>"
sftp_remote_path = "/path/to/files"

# Azure Cosmos DB connection details
cosmos_db_endpoint = "<your-cosmos-db-endpoint>"
cosmos_db_key = "<your-cosmos-db-key>"
cosmos_db_database_name = "<your-database-name>"
cosmos_db_container_name = "<your-container-name>"

def extract_sftp_info():
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

def store_in_cosmos_db(files_info, **kwargs):
    client = CosmosClient(cosmos_db_endpoint, cosmos_db_key)
    database = client.get_database_client(cosmos_db_database_name)
    container = database.get_container_client(cosmos_db_container_name)

    for file_info in files_info:
        container.upsert_item(file_info)

# Airflow DAG configuration
dag = DAG(
    'sftp_to_cosmosdb',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

# Define tasks in the DAG
extract_sftp_task = PythonOperator(
    task_id='extract_sftp_info',
    python_callable=extract_sftp_info,
    dag=dag,
)

store_in_cosmos_db_task = PythonOperator(
    task_id='store_in_cosmosdb',
    provide_context=True,
    python_callable=store_in_cosmos_db,
    dag=dag,
)

# Define task dependencies
extract_sftp_task >> store_in_cosmos_db_task