# import CosmosOperations class
from write_cosmos import CosmosOperations

# create object of class
cosmosOperations = CosmosOperations()

# Creating connection to cosmosdb
client, database_name, container_name, update_key, delete_key = cosmosOperations.client_init()

# Create a database
cosmosOperations.create_database(client, database_name)

# Create a container
cosmosOperations.create_container(client, database_name, container_name)

# read event_gird data
event_data = cosmosOperations.read_data("C:/Users/srika/Documents/Azure_Code/data")

# Insert data
cosmosOperations.insert_data(client, database_name, container_name, event_data)

# Query data
queried_data = cosmosOperations.query_data(client, database_name, container_name,
                                               "SELECT * FROM c WHERE c.app_code='sample_app'")
print(queried_data)

# Update data (modify the event_data dictionary and use upsert to update)
event_data["sft_batch_file_name"] = "batch124"
cosmosOperations.update_data(client, database_name, container_name, event_data)

# Delete data
cosmosOperations.delete_data(client, database_name, container_name, "1", "sample_app")

