import subprocess
import sys
import json
import psycopg2
from psycopg2 import sql

# global variable declaration for constant values
host_name = 'npe'
dbname = ''
user_name = ''
password = ''
port_no = ''
meta_tbl = ''
job_name = "None"
global jar_path, main_class, databricks_mode, cosmos_conf_name, existing_cluster_id, \
    secret_scope, update_timestamp, bronze_schedule, silver_schedule, \
    gold_schedule, workflow_database, workflow_container, timezone


def connect_to_database(dbname):
    try:
        connection = psycopg2.connect(database=dbname, user=user_name, password=password,
                                      host=host_name, port=port_no)
        return connection
    except psycopg2.Error as e:
        print(f"unable to connect to the database, Error: {e}")
        return None


def retrieve_metadata(connection, meta_tbl):
    try:
        cursor = connection.cursor()
        query = sql.SQL("SELECT json_data FROM {} ORDER BY id ASC ").format(
            sql.Identifier(meta_tbl)
        )
        cursor.execute(query)
        rows = cursor.fetchone()
        cursor.close()

        for row in rows:
            extract_metadata_values(row)
        connection.close()
    except psycopg2.Error as e:
        print(f"Error retrieving JSON data from the table. Error:{e}")


def extract_metadata_values(row):
    global jar_path, main_class, databricks_mode, cosmos_conf_name, \
        existing_cluster_id, secret_scope, update_timestamp, bronze_schedule, \
        silver_schedule, gold_schedule, workflow_database, workflow_container, timezone

    jar_path = row['jar_path']
    main_class = row['main_class']
    databricks_mode = row['databricksMode']
    cosmos_conf_name = row['cosmosConfName']
    secret_scope = row['secretScope']
    existing_cluster_id = row['existing_cluster_id']
    metadata_database = row['metadata_db']
    config_meta_container = row['meta_container']
    config_db = row['config_db']
    config_container = row['config_container']
    timestamp_container = row['latest_timestamp']
    workflow_container = row['workflow_jobcontainer']
    workflow_database = row['workflow_jobdb']
    bronze_schedule = row['bronze_scheduler']
    silver_schedule = row['silver_scheduler']
    gold_schedule = row['gold_scheduler']
    timezone = row['timezone']

    metadb = connect_to_database(metadata_database)
    last_load_timestamp = get_last_processed_timestamp(metadb, timestamp_container)
    print("last_load_timestamp:", last_load_timestamp)

    data_items = get_uniqueid_info(last_load_timestamp, metadb, config_meta_container)
    print("The unique data", data_items)

    for ids in data_items:
        update_timestamp = ids[1]
        database = connect_to_database(config_db)
        process_uid_data(ids, database, config_container)

    print("The updated_timestamp values:", update_timestamp)
    update_last_processed_timestamp(update_timestamp, metadb, timestamp_container)


def get_last_processed_timestamp(metadb, timestamp_container):
    try:
        cursor = metadb.cursor()
        # sql query to select the entire json column data
        query = (sql.SQL("SELECT latest_timestamp from {} order by id DESC LIMIT 1").
                 format(sql.Identifier(timestamp_container))
                 )
        print(query)
        cursor.execute(query)
        # fetching all rows
        rows = cursor.fetchone()
        print(rows)

        print(rows[0])
        return rows[0] if rows else 0
    except psycopg2.Error as e:
        print(f"Error retrieving Json data from the table Error:{e}")


def get_uniqueid_info(last_load_timestamp, metadb, config_meta_container):
    # selecting meta data information in meta table
    try:
        cursor = metadb.cursor()
        # sql query to selecting meta table data
        query = sql.SQL("SELECT uid, insert_timestamp from {} where insert_timestamp > {}  ").format(
            sql.Identifier(config_meta_container), sql.Literal(last_load_timestamp)
        )
        cursor.execute(query)
        # fetching all rows
        rows = cursor.fetchall()
        print(rows)
        if any(rows):
            return rows
        else:
            print("The latest Unique ids are not presented")
            exit(1)
    except psycopg2.Error as e:
        print(f"Error retrieving Json data from the table. Error: {e}")


def process_uid_data(ids, database, config_container):
    with database.cursor() as cursor:
        query = sql.SQL("SELECT json_data FROM {} WHERE uid LIKE {}").format(
            sql.Identifier(config_container),
            sql.Literal(ids[0] + '%')
        )
        print(query)
        cursor.execute(query)
        rows = cursor.fetchall()
        print(rows)
        if any(rows):
            for row in rows:
                json_data = row[0]
                process_json_data(json_data)
        else:
            print("The Unique ids are not matching")
    database.close()


def process_json_data(json_data):
    if 'bronze' in json_data:
        bronze_values = json_data['bronze']
        print("Bronze Values:", bronze_values)
        for items in bronze_values:
            job_name = items['workflowName']
            task_file_path = items['filename']
            config_folder = items['filePath']
            args = [
                f"databricksMode={databricks_mode}",
                f"cosmosConfName={cosmos_conf_name}",
                f"secretScope={secret_scope}",
                f"taskFilePath={task_file_path}",
                f"configFolder={config_folder}"
            ]
            job_schedule_config(job_name, args, bronze_schedule)

    if 'silver' in json_data:
        silver_values = json_data['silver']
        print("Silver Values:", silver_values)
        for items in silver_values:
            job_name = items['workflowName']
            task_file_path = items['filename']
            config_folder = items['filePath']
            if task_file_path.lower().endswith('.json'):
                args = [
                    f"databricksMode={databricks_mode}",
                    f"cosmosConfName={cosmos_conf_name}",
                    f"secretScope={secret_scope}",
                    f"taskFilePath={task_file_path}",
                    f"configFolder={config_folder}"
                ]
                job_schedule_config(job_name, args, silver_schedule)

    if 'gold' in json_data:
        gold_values = json_data['gold']
        print("Gold Values:", gold_values)
        for items in gold_values:
            job_name = items['workflowName']
            task_file_path = items['filename']
            config_folder = items['filePath']
            if task_file_path.lower().endswith('.json'):
                args = [
                    f"databricksMode={databricks_mode}",
                    f"cosmosConfName={cosmos_conf_name}",
                    f"secretScope={secret_scope}",
                    f"taskFilePath={task_file_path}",
                    f"configFolder={config_folder}"
                ]
                job_schedule_config(job_name, args, gold_schedule)


def job_schedule_config(job_name, args, job_schedule):
    print("the jobname", job_name)
    job_config = {
        "name": job_name,
        "existing_cluster_id": existing_cluster_id,
        "libraries": [{"jar_url": jar_path}],
        "spark_jar_task": {
            "main_class_name": main_class,
            "parameters": args,
            # "jar_url": jar_path
        },
        "schedule": {
            "quartz_cron_expression": job_schedule,
            "timezone_id": timezone
        }
    }
    create_and_run_databricks_job(job_config, job_name)


def create_and_run_databricks_job(job_info, job_name):
    # Convert the config_data dictionary to a JSON string
    config_data_json = json.dumps(job_info)
    print("config_data_json", config_data_json)

    create_job_command = ["databricks", "jobs", "create", "--json", config_data_json]
    response = subprocess.run(create_job_command, capture_output=True, text=True)

    if response.returncode != 0:
        print("stderr:", response.stderr)
        print("stdout:", response.stdout)
        print("std return:", response.returncode)
        raise Exception(f"Error creating job: {response.stderr}")

    # Parse the job ID from the response
    response_json = json.loads(response.stdout)
    job_id = response_json["job_id"]

    run_job_command = ["databricks", "jobs", "run-now", "--job-id", str(job_id)]
    response = subprocess.run(run_job_command, capture_output=True, text=True)

    if response.returncode != 0:
        raise Exception(f"Error running job: {response.stderr}")

    print(response.stdout)
    # return job_id
    insert_data_into_postdb(job_id, job_name)


def insert_data_into_postdb(job_id, job_name):
    workflow_db = connect_to_database(workflow_database)
    new_item = {
        'id': str(job_id),
        'job_name': job_name,
        'dependence': 'info_sftp'
    }
    try:
        cursor = workflow_db.cursor()
        # Converting python dictionary to a JSON string
        json_data_str = json.dumps(new_item)

        # SQL query to insert new JSON data
        query = sql.SQL("INSERT INTO {} (json_data) VALUES (%s)").format(
            sql.Identifier(workflow_container)
        )

        # Executing the query with the JSON data string
        cursor.execute(query, (json_data_str,))
        # Commiting the changes
        workflow_db.commit()
        # Closing the cursor
        cursor.close()
        print(f"Item with job_id {job_id}, Job name {job_name} inserted successfully")
    except psycopg2.Error as e:
        print(f"Error inserting new JSON data into the table. Error: {e}")


def update_last_processed_timestamp(update_timestamp, metadb, timestamp_container):
    try:
        with metadb.cursor() as cursor:
            # query to fetching latest timestamp table
            query_select = f"SELECT * FROM {timestamp_container}"
            cursor.execute(query_select)
            rows = cursor.fetchall()
            # print(rows)

            if rows:
                for row in rows:
                    # updating the existing row
                    print(row[0])
                    query_update = f"UPDATE {timestamp_container} SET latest_timestamp = %s WHERE id = %s"
                    cursor.execute(query_update, (update_timestamp, row[0]))
                    print("The latest timestamp updated in DB")
            else:
                # creating a new row if it doesn't exist
                query_insert = f"INSERT INTO {timestamp_container} (id, latest_timestamp) VALUES (%s, %s)"
                cursor.execute(query_insert, ('uniqueid_document_id', update_timestamp))
                print(" A new row with the latest timestamp has been inserted into DB")
        # committing the changes
        metadb.commit()
    finally:
        # db connection close
        metadb.close()


if __name__ == "__main__":
    # connecting to the postgreSQL database
    connection = connect_to_database(dbname)

    if connection:
        table_name = 'constant_table'
        # Retrieving the constant table data
        retrieve_metadata(connection, table_name)
        # connection.close()
    else:
        print("Failed to fetch Postgress Database")
        sys.exit(1)
