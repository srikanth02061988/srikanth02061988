from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json

filename = "sample.json"

container_name="test"
constr = ""

blob_service_client = BlobServiceClient.from_connection_string(constr)
container_client = blob_service_client.get_container_client(container_name)
blob_client = container_client.get_blob_client(filename)
streamdownloader = blob_client.download_blob()

fileReader = json.loads(streamdownloader.readall())
print(fileReader)
