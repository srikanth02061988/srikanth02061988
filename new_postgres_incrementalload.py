import psycopg2
from psycopg2 import sql

# global variable declaration for constant values
host_name = 'npe'
database_name = ''
user_name = ''
password = ''
port_no = ''
meta_tbl = ''
job_name = "None"


def retrieve_metadata(connection, meta_tbl):
    try:
        cursor = connection.cursor()
        query = sql.SQL("SELECT json_data FROM {} ORDER BY id ASC ").format(
            sql.Identifier(meta_tbl)
        )
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()

        for row in rows:
            extract_metadata_values(row)
        connection.close()
    except psycopg2.Error as e:
        print(f"Error retrieving JSON data from the table. Error:{e}")


def extract_metadata_values(row):
    global jar_path, main_class, databricks_mode, cosmos_conf_name
    global existing_cluster_id, secret_scope, update_timestamp=0
    global bronze_schedule, silver_schedule, gold_schedule
    global workflow_database,workflow_container,timezone

    jar_path = row['jar_path']
    main_class = row['main_class']
    databricks_mode = row['databricksMode']
    cosmos_conf_name = row['cosmosConfName']
    secret_scope = row['secretScope']
    existing_cluster_id = row['existing_cluster_id']
    metadata_database = row['metadata_db']
    config_meta_container = row['meta_container']
    config_db = row['config_db']
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
        process_uid_data(ids, database,timestamp_container)


def process_uid_data(ids, database,timestamp_container):
    with database.cursor() as cursor:
        query = sql.SQL("SELECT json_data FROM {} WHERE uid LIKE {}").format(
            sql.Identifier(timestamp_container),
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
            job_schedule_config(args,bronze_schedule)

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
                job_schedule_config(args, silver_schedule)

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
                job_schedule_config(args, gold_schedule)

    print("The updated_timestamp values:", update_timestamp)
    update_last_processed_timestamp(update_timestamp, metadb, timestamp_container)


def job_schedule_config(args,job_schedule):
    print("the jobname", job_name)
    job_config = {
        "name": job_name,
        "existing_cluster_id": existing_cluster_id,
        "spark_jar_task": {
            "main_class_name": main_class,
            "parameters": args,
            "jar_url": jar_path
        },
        "schedule": {
            "quartz_cron_expression": job_schedule,
            "timezone_id": timezone
        }
    }
    create_and_run_databricks_job(job_config)
