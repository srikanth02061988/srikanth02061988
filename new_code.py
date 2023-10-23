from azure.eventgrid import EventGridConsumer
from azure.cosmos import CosmosClient, PartitionKey

# Initialize Cosmos DB client
endpoint = '<YOUR_COSMOSDB_ENDPOINT>'
key = '<YOUR_MASTER_KEY>'
client = CosmosClient(endpoint, key)

# Define database and container names
database_name = 'SampleDatabase'
container_name = 'SampleContainer'

# Create a new database
database = client.create_database_if_not_exists(id=database_name)

# Create a new container
container = database.create_container_if_not_exists(
    id=container_name, 
    partition_key=PartitionKey(path="/partitionKey"),
    offer_throughput=400
)

def create_item(app_code, sftp_batch_file_name, date_time_stamp, complete_directory_structure):
    key = f"{app_code}{sftp_batch_file_name}{date_time_stamp}"
    value = complete_directory_structure

    container.create_item(body={'id': key, 'value': value})

def read_item(key):
    response = container.read_item(item=key, partition_key=key)
    return response

def update_item(key, new_value):
    response = container.replace_item(item=key, body={'id': key, 'value': new_value})
    return response

def delete_item(key):
    response = container.delete_item(item=key, partition_key=key)
    return response

def trigger_azure_function():
    print("Triggering Azure Function")

def trigger_airflow_job():
    print("Triggering Airflow Job")

def event_handler(event):
    event_data = event.data

    # Extract required information from the event_data
    app_code = event_data['app_code']
    sftp_batch_file_name = event_data['sftp_batch_file_name']
    date_time_stamp = event_data['date_time_stamp']
    complete_directory_structure = event_data['complete_directory_structure']

    # Create a key-value pair using the information
    key = f"{app_code}{sftp_batch_file_name}{date_time_stamp}"

    # Store in Cosmos DB
    create_item(app_code, sftp_batch_file_name, date_time_stamp, complete_directory_structure)

    # Example: Read the item
    read_response = read_item(key)
    print(f"Read response: {read_response}")

    # Example: Update the item
    update_response = update_item(key, "New Value")
    print(f"Update response: {update_response}")

    # Example: Delete the item
    delete_response = delete_item(key)
    print(f"Delete response: {delete_response}")

    # Trigger Azure Function to listen for new events in Cosmos DB
    trigger_azure_function()

    # Trigger Airflow job for Databricks to read Azure Blob Storage
    trigger_airflow_job()

consumer = EventGridConsumer()

# Add your Event Grid topic endpoint and key here
for event in consumer.iterate_over_events('<YOUR_EVENT_GRID_TOPIC_ENDPOINT>', '<YOUR_EVENT_GRID_KEY>'):
    event_handler(event)