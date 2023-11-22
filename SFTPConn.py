pip install pysftp

import pysftp
from azure.cosmos import CosmosClient
import os
import datetime

# SFTP server credentials
sftp_host = 'your_sftp_host'
sftp_port = 22
sftp_username = 'your_sftp_username'
sftp_password = 'your_sftp_password'
sftp_remote_path = 'sftp server path'

# Cosmos DB credentials
cosmosdb_endpoint = 'your_cosmosdb_endpoint'
cosmosdb_key = 'your_cosmosdb_key'
cosmosdb_database_name = 'your_database_name'
cosmosdb_container_name = 'your_container_name'

# Connect to Cosmos DB
cosmos_client = CosmosClient(cosmosdb_endpoint, cosmosdb_key)
database = cosmos_client.get_database_client(cosmosdb_database_name)
container = database.get_container_client(cosmosdb_container_name)

def upload_file_and_log(file_path):
    try:
        # Connect to SFTP server
        with pysftp.Connection(sftp_host, username=sftp_username, password=sftp_password, port=sftp_port) as sftp:
            # Upload the file to the SFTP server
            sftp.put(file_path, sftp_remote_path + os.path.basename(file_path))

            # Get information about the uploaded file
            file_stat = os.stat(file_path)
            file_size = file_stat.st_size
            file_name = os.path.basename(file_path)
            upload_time = datetime.datetime.now().isoformat()

            # Log the details to Cosmos DB
            document = {
                'fileName': file_name,
                'fileSize': file_size,
                'uploadTime': upload_time,
                'uploadedBy': sftp_username
            }

            container.create_item(body=document)

            print(f"File '{file_name}' uploaded successfully. Details logged to Cosmos DB.")

    except Exception as e:
        print(f"Error: {e}")

# Example usage
file_to_upload = 'file path location path'
upload_file_and_log(file_to_upload)



import paramiko
import os
from datetime import datetime

# Azure VM (SFTP server) credentials
sftp_host = 'your_azure_vm_ip'
sftp_port = 22
sftp_username = 'your_sftp_username'
sftp_password = 'your_sftp_password'
sftp_remote_path = 'your vm login'

# Log file details
log_file_path = 'upload_log.txt'

def log_upload_details(username, filename, file_size):
    # Log the details to a file
    with open(log_file_path, 'a') as log_file:
        log_file.write(f"{datetime.now().isoformat()} - User: {username}, File: {filename}, Size: {file_size} bytes\n")

def sftp_upload_file(username, local_file_path):
    try:
        # Connect to SFTP server
        transport = paramiko.Transport((sftp_host, sftp_port))
        transport.connect(username=sftp_username, password=sftp_password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Upload the file
        remote_file_path = sftp_remote_path + os.path.basename(local_file_path)
        sftp.put(local_file_path, remote_file_path)

        # Get file details
        file_size = sftp.stat(remote_file_path).st_size
        filename = os.path.basename(local_file_path)

        # Log the upload details
        log_upload_details(username, filename, file_size)

        print(f"File '{filename}' uploaded successfully by user '{username}'.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the SFTP connection
        if sftp:
            sftp.close()
        if transport:
            transport.close()

# Example usage
file_to_upload = 'path/to/your/file.txt'
uploading_user = 'your_azure_vm_sftp_username'
sftp_upload_file(uploading_user, file_to_upload)

