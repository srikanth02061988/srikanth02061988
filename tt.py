import pandas as pd
import json
from azure.cosmos import CosmosClient, PartitionKey, exceptions
import os
import glob


class CosmosOperations:
    @staticmethod
    def client_init():
        with open('config.json') as config_file:
            config = json.load(config_file)

        endpoint = config["endpoint"]
        primary_key = config["primary_key"]
        database_name = config["database_name"]
        container_name = config["container_name"]
        update_key = config['update_key']
        delete_key = config['delete_key']

        client = CosmosClient(endpoint, credential=primary_key)
        return client, database_name, container_name, update_key, delete_key

    @staticmethod
    def create_database(client, db_name):
        try:
            database = client.create_database(db_name)
            print('Database created')
            return database
        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 409:  # Conflict (database already exists)
                database = client.get_database_client(db_name)
                return database
            else:
                raise

    @staticmethod
    def create_container(client, db_name, container_name):
        database = client.get_database_client(db_name)

        try:
            container = database.create_container(id=container_name,
                                                  partition_key=PartitionKey(path="/partitionKey"),
                                                  offer_throughput=400)
            print('Container created')
            return container
        except exceptions.CosmosHttpResponseError as e:
            if e.status_code == 409:  # Conflict (container already exists)
                container = database.get_container_client(container_name)
                return container
            else:
                raise

    def read_data(filepath):
        # checking if the directory exists or not
        if os.path.exists(filepath):
            # checking if the directory is empty or not
            if len(os.listdir(filepath)) == 0:
                return "No Files Found in the directory"
            else:
                json_pattern = os.path.join(filepath,'*.json')
                file_list = glob.glob(json_pattern)
                for file in file_list:
                    with open(file, 'r') as json_file:
                        data = json.load(json_file)
                        return data

    @staticmethod
    def insert_data(client, database_name, container_name, item):
        container = client.get_database_client(database_name).get_container_client(container_name)
        container.upsert_item(item)
        print('Data inserted')

    @staticmethod
    def query_data(client, database_name, container_name, query):
        container = client.get_database_client(database_name).get_container_client(container_name)
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        return items

    @staticmethod
    def update_data(client, database_name, container_name, item):
        container = client.get_database_client(database_name).get_container_client(container_name)
        container.upsert_item(item)
        print('Data updated')

    @staticmethod
    def delete_data(client, database_name, container_name, item_id, partition_value):
        container = client.get_database_client(database_name).get_container_client(container_name)
        container.delete_item(item_id, partition_key=partition_value)
        print('Data deleted')



def main():
    client, database_name, container_name, update_key, delete_key = CosmosOperations.client_init()

    # Create a database
    CosmosOperations.create_database(client, database_name)

    # Create a container
    CosmosOperations.create_container(client, database_name, container_name)

    # read event_gird data
    event_data = CosmosOperations.read_data("C:/Users/srika/Documents/Azure_Code/data")


    # Sample Data
    # event_data = {
    #     "id": "1",
    #     "app_code": "sample_app",
    #     "sft_batch_file_name": "batch123",
    #     "data_time_stamp": "2021-09-14T12:34:56Z",
    #     "complete_directory_structure": "/path/to/directory",
    #     "partitionKey": "sample_app"
    # }

    # Insert data
    CosmosOperations.insert_data(client, database_name, container_name, event_data)

    # Query data
    queried_data = CosmosOperations.query_data(client, database_name, container_name,
                                               "SELECT * FROM c WHERE c.app_code='sample_app'")
    print(queried_data)

    # Update data (modify the event_data dictionary and use upsert to update)
    event_data["sft_batch_file_name"] = "batch124"
    CosmosOperations.update_data(client, database_name, container_name, event_data)

    # Delete data
    CosmosOperations.delete_data(client, database_name, container_name, "1", "sample_app")


if __name__ == '__main__':
    main()