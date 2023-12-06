from azure.cosmos import CosmosClient
from datetime import datetime

# Set your Cosmos DB endpoint and key
endpoint = 'your_cosmos_db_endpoint'
key = 'your_cosmos_db_key'

# Create a Cosmos DB client
client = CosmosClient(endpoint, key)

# Specify the database and container
database_name = 'your_database_name'
container_name = 'your_container_name'

# Get a reference to the database and container
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Specify the last_load_timestamp in Unix timestamp format
last_load_timestamp_unix = 1641043200  # Replace with your actual timestamp

# Convert the Unix timestamp to a datetime string
last_load_datetime = datetime.utcfromtimestamp(last_load_timestamp_unix).strftime('%Y-%m-%dT%H:%M:%SZ')

# Query to get records modified or created after the last load timestamp
query = f"SELECT * FROM c WHERE c.timestamp_field > '{last_load_datetime}'"

# Execute the query
results = container.query_items(query=query, enable_cross_partition_query=True)

# Check if the unique ID is present
if next(results, None):
    # Unique ID is present, proceed with further processing
    print(f"Unique ID '{desired_unique_id}' found in Cosmos DB.")
    # Process the new or modified records
    for item in results:
      # Process the record as needed
      print(item)
    
      # Update last_load_timestamp if necessary
      item_timestamp = datetime.strptime(item['timestamp_field'], '%Y-%m-%dT%H:%M:%SZ')
      last_load_timestamp_unix = max(last_load_timestamp_unix, int(item_timestamp.timestamp()))

      # Update your logic to store last_load_timestamp_unix for the next incremental load
      print(f"Update your logic to store last_load_timestamp_unix: {last_load_timestamp_unix}")
    
    # Add your logic for further processing here
    
else:
    # Unique ID is not present, display an error message
    print(f"Error: Unique ID '{desired_unique_id}' not found in Cosmos DB.")

