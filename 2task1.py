import os
import json
import requests
from azure.cosmos import CosmosClient
from databricks_cli.sdk import JobsService

def get_cosmosdb_config():
    # Replace with your CosmosDB configuration
    cosmosdb_endpoint = "your_cosmosdb_endpoint"
    cosmosdb_key = "your_cosmosdb_key"
    database_id = "your_database_id"
    collection_id = "your_collection_id"

    client = CosmosClient(cosmosdb_endpoint, cosmosdb_key)
    db = client.get_database_client(database_id)
    container = db.get_container_client(collection_id)

    # Query CosmosDB for configuration data
    query = "SELECT * FROM c WHERE c.type = 'databricks_config'"
    items = list(container.query_items(query, enable_cross_partition_query=True))
    if not items:
        raise Exception("No Databricks configuration found in CosmosDB")

    # Assuming you have only one config record, you can retrieve it like this
    return items[0]

def create_databricks_job(config):
    # Extract Databricks configuration from the CosmosDB document
    databricks_endpoint = config['databricks_endpoint']
    token = config['databricks_token']
    cluster_id = config['cluster_id']

    # Define your Databricks job settings (customize this)
    job_settings = {
        "name": "YourDatabricksJobName",
        "new_cluster": {
            "cluster_id": cluster_id,
            # Additional cluster configuration
        },
        "spark_jar_task": {
            "main_class_name": "your.main.class",
            "parameters": ["param1", "param2"],
            "jar_uri": "dbfs:/path/to/your.jar",
        },
        # Additional job configuration
    }

    # Create the Databricks job using Databricks REST API
    headers = {
        "Authorization": f"Bearer {token}"
    }
    url = f"{databricks_endpoint}/api/2.0/jobs/create"
    response = requests.post(url, headers=headers, json=job_settings)

    if response.status_code == 200:
        job_info = response.json()
        print(f"Created Databricks job with job_id: {job_info['job_id']}")
    else:
        print(f"Failed to create Databricks job. Status code: {response.status_code}, Response: {response.text}")

def main():
    cosmosdb_config = get_cosmosdb_config()
    create_databricks_job(cosmosdb_config)

if __name__ == "__main__":
    main()
