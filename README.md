- 👋 Hi, I’m @srikanth02061988
- 👀 I’m interested in ...
- 🌱 I’m currently learning ...
- 💞️ I’m looking to collaborate on ...
- 📫 How to reach me ...

<!---
srikanth02061988/srikanth02061988 is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->

 

import pandas as pd

from azure.cosmosdb.table.tableservice import TableService

from azure.cosmosdb.table.models import Entity

import os
import sys
import glob

parent_dir = os.path.dirname(__file__)
sys.path.append(parent_dir)


table_service = TableService(account_name='xxxx', account_key='xxxx')
table_name = table_service.create_table("table_name")


# Python program to read csv files & writing into cosmos db.

def read_csvfiles(directoryPath):
    # reading csv files
    os.chdir(directoryPath)
    all_files = glob.glob('*.csv')
    data =[]
    for file in all_files:
        row = pd.read_csv(file,  header=None)
        data.append(row)
    print(data)
    write_cosmosdb(data)



def write_cosmosdb(table):
    # table_service = TableService(account_name='xxxx', account_key='xxxx')
    # table_service.create_table(table - name)
    index=0
    for row in table:
        task = {'PartitionKey': "P"+str(index), 'RowKey':  "R"+str(index+1)}
        index=index+1
        for ele in row:
            task["Row"+str(row.index(ele))]=ele
        table_service.insert_entity(table_name, task)



# Function for checking if the directory
# contains file or not
def isEmpty(directoryPath):
    # Checking if the directory exists or not
    if os.path.exists(directoryPath):

        # Checking if the directory is empty or not
        if len(os.listdir(directoryPath)) == 0:
            return "No files found in the directory."
        else:
            read_csvfiles(directoryPath)
    else:
        return "Directory does not exist !"


def main():
    print("Hello World!")
    # Valid directory path
    directoryPath = "D://softwares/GCP/Flow/ProfDataEngineer/GCPCode/GCP inteview/New folder"
    isEmpty(directoryPath)


if __name__ == "__main__":
    main()

 
