import requests
from datetime import datetime
from azure.cosmos import CosmosClient

# Databricks API endpoint and token
databricks_api_url = "https://<databricks-instance>/api/2.0/jobs/list"
databricks_token = "<your-databricks-api-token>"

# Azure Cosmos DB connection details
cosmos_db_endpoint = "<your-cosmos-db-endpoint>"
cosmos_db_key = "<your-cosmos-db-key>"
cosmos_db_database_name = "<your-database-name>"
cosmos_db_container_name = "<your-container-name>"

# Make a request to Databricks API
headers = {"Authorization": f"Bearer {databricks_token}"}
response = requests.get(databricks_api_url, headers=headers)
job_data = response.json()

# Process and extract relevant information
jobs_info = []
for job in job_data["jobs"]:
    job_id = job["job_id"]
    start_time = datetime.fromtimestamp(job["start_time"] / 1000.0)
    end_time = datetime.fromtimestamp(job["end_time"] / 1000.0) if "end_time" in job else None
    duration = job.get("duration", None)
    status = job["state"]

    jobs_info.append({
        "job_id": job_id,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "status": status
    })

# Connect to Cosmos DB
client = CosmosClient(cosmos_db_endpoint, cosmos_db_key)
database = client.get_database_client(cosmos_db_database_name)
container = database.get_container_client(cosmos_db_container_name)

# Insert job information into Cosmos DB container
for job_info in jobs_info:
    container.upsert_item(job_info)