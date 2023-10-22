import pandas as pd
import json
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.documents as documents
import azure.cosmos.http_constants as http_constants

# Load config file
with open('config.json') as config_file:
    config = json.load(config_file)

class ReadCosmos:
    @staticmethod
    def clientinit():
        client = cosmos_client.CosmosClient(url_connection=config["endpoint"], auth={"masterKey": config["primarykey"]})
        return client

    @staticmethod
    def create_cosmosdb(client, database_name):
        try:
            client.CreateDatabase({'id': database_name})
            print(f'{database_name} database created.')
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                print(f"{database_name} already exists")
            else:
                raise e

    @staticmethod
    def create_container(client, database_name, container_name, partition_key):
        database_link = f'dbs/{database_name}'
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
            print(f'{container_name} Container created')
        except errors.HTTPFailure as e:
            if e.status_code == http_constants.StatusCodes.CONFLICT:
                container = client.ReadContainer(f'dbs/{database_name}/colls/{container_definition["id"]}')
            else:
                raise e

    @staticmethod
    def insert_data(client, df, database_name, container_name):
        collection_link = f'dbs/{database_name}/colls/{container_name}'
        for i in range(0, df.shape[0]):
            data_dict = df.iloc[i, :].to_dict()
            client.UpsertItem(collection_link, data_dict)
        print('Records inserted successfully.')

    @staticmethod
    def query_data(client, database_name, container_name, query):
        dflist = []
        collection_link = f'dbs/{database_name}/colls/{container_name}'
        for item in client.QueryItems(collection_link, query, {'enableCrossPartitionQuery': True}):
            dflist.append(dict(item))
        df = pd.DataFrame(dflist)
        print('Query successful.')
        return df

    @staticmethod
    def delete_data(client, database_name, container_name, query, deviceid):
        collection_link = f'dbs/{database_name}/colls/{container_name}'
        items = list(client.QueryItems(collection_link, query, {'enableCrossPartitionQuery': True}))
        for item in items:
            client.DeleteItem(item['_self'])

    @staticmethod
    def update_data(client, database_name, container_name, query):
        document_link = f'dbs/{database_name}/colls/{container_name}'
        for item in client.QueryItems(document_link, 'SELECT * FROM ' + container_name, {'enableCrossPartitionQuery': True}):
            item['created'] += '123'
            client.ReplaceItem(item['_self'], item)

class ReadCsvFiles:
    @staticmethod
    def data():
        df = pd.read_csv('file path', encoding='ISO-8859–1', dtype='str')
        df = df.reset_index()
        return df

def main():
    with ReadCosmos.clientinit() as client:
        try:
            ReadCosmos.create_cosmosdb(client, database_name)
            ReadCosmos.create_container(client, database_name, container_name, partition_key)
            df = ReadCsvFiles.data()
            ReadCosmos.insert_data(client, df, database_name, container_name)
            query_data = ReadCosmos.query_data(client, database_name, container_name, query)
            ReadCosmos.delete_data(client, database_name, container_name, query, 'deviceid')
            ReadCosmos.update_data(client, database_name, container_name, query)
        except Exception as e:
            print(f'Error: {e}')

if _name_ == "_main_":
    main()