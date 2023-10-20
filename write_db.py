# Online Python compiler (interpreter) to run Python online.
# Write Python 3 code in this online editor and run it.
print("Hello world")
import pandas as pd

import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents

import azure.cosmos.http_constants as http_constants


print('Imported packages successfully.')

# Load config file
with open('config.json') as config_file:
    config = json.load(config_file)


class IDisposable(cosmos_client.CosmosClient):
    """ A context manager to automatically close an object with a close method
    in a with statement. """

    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj  # bound to target

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.obj = None


class ReadCosmos:

    # Creating the cosmos client
    def clientinit():

        client = cosmos_client.CosmosClient(url_connection=config["endpoint"], auth={"masterKey": config["primarykey"]})
        return client
        print('Client initialized.')

    # creating a Cosmos DB database
    def create_cosmosdb(client, database_name):

        try:
            database = client.CreateDatabase({'id': database_name})
            print('{} database created.'.format(database_name))
        except errors.HTTPFailure:
            print("{} already exists".format(database_name))

    # Create a document collection / container
    def create_container(client, database_name, container_name, partition_key):

        database_link = 'dbs/' + database_name
        container_definition = {'id': container_name,
                                'partitionKey':
                                    {
                                        'paths': ['/country'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                                }
        try:
            container = client.CreateContainer(database_link=database_link,
                                               collection=container_definition,
                                               options={'offerThroughput': 400})
            print('{} Container created'.format(container_name))
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                container = client.ReadContainer("dbs/" + database['id'] + "/colls/" + container_definition['id'])
            else:
                raise e

        # Write data to the container

    def insert_data(client, df, database_name, container_name):

        # Create Connection Link string
        collection_link = 'dbs/' + database_name + '/colls/' + container_name

        # Write rows of a pandas DataFrame as items to the Database Container
        for i in range(0, df.shape[0]):
            # Create dictionary of row being passed to the for loop
            data_dict = dict(df.iloc[i, :])
            # Convert that dictionary to json record
            data_dict = json.dumps(data_dict)
            # Insert the json record into the Azure Cosmos Db
            insert_data = client.UpsertItem(collection_link, json.loads(data_dict))
        print('Records inserted successfully.')

        # Query data from the container

    def query_data(client, database_name, container_name, query):
        # Initialize list
        dflist = []
        # Create Connection Link string
        collection_link = 'dbs/' + database_name + '/colls/' + container_name

        # For-loop to retrieve individual json records from Cosmos DB
        # that satisfy our query
        for item in client.QueryItems(collection_link,
                                      query,
                                      {'enableCrossPartitionQuery': True}
                                      ):
            # Append each item as a dictionary to list
            dflist.append(dict(item))

        # Convert list to pandas DataFrame
        df = pd.DataFrame(dflist)
        print('Query successful.')
        return df
    
    # Query data from the container

    def delete_data(client, database_name, container_name, query,deviceid):
        
         #fetch items
        query = f"SELECT * FROM c WHERE c.device.deviceid IN ('{deviceid}')"
        items = list(container.query_items(query=query, enable_cross_partition_query=False))
        for item in items:
            container.delete_item(item, 'partition-key')
        
    def update_data(client, database_name, container_name, query):
        
        document_link = "dbs/" + COSMOS_DB_DATABASE_ID + "/colls/" + COSMOS_DB_COLLECTION_ID
        for item in client.QueryItems(document_link,
                                  'SELECT * FROM ' + COSMOS_DB_COLLECTION_ID,
                                  {'enableCrossPartitionQuery': True}):
                                      item['created'] += '123'
                                      print(json.dumps(item, indent=True))
                                      client.ReplaceItem(document_link, item, options=None)
        



class ReadCsvFiles:

    def data():
        # Download and read csv file
        df = pd.read_csv('file path', encoding='ISO-8859–1',
                         dtype='str')
        # Reset index - creates a column called 'index'
        df = df.reset_index()

        return df


database_name = 'HDIdatabase2'
container_name = 'HDIcontainer2'
partition_key = 'country'
query = 'SELECT * FROM c where c.country="Afghanistan" and c.level="National"'


def main():
    with IDisposable(cosmos_client.CosmosClient(url_connection=config["endpoint"],
                                                auth={"masterKey": config["primarykey"]})) as client:

        try:

            # Creating Database
            ReadCosmos.create_cosmosdb(client, database_name)

            # Creating Container
            ReadCosmos.create_container(client, database_name, container_name, partition_key)

            # Reading csv the data
            df = ReadCsvFiles.data()

            # Inserting the data cosmos db
            ReadCosmos.insert_data(client, df, database_name, container_name)

            # Querying the data
            query_data = ReadCosmos.query_data(client, database_name, container_name, query)
            
            # deleting the data
            query_data = ReadCosmos.query_data(client, database_name, container_name, query)

	    # update the data
            query_data = ReadCosmos.query_data(client, database_name, container_name, query)
            

        except Exception as e:

            print('You have errors.')


if __name__ == "__main__":
    main()

 