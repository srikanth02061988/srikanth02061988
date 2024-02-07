def process_json_data(uid, json_data):
    workflow_tasks = []
    job_name = f"{json_data['workflowName']}_{uid}"

    for layer in ('bronze', 'silver', 'gold'):
        if layer in json_data:
            for item in json_data[layer]:
                # job_name = item['workflowName'] + '_' + f"{uid}"
                task_name = item['workflowName'] + '_' + layer
                task_file_path = item['filename']
                config_folder = item['filePath']
                blob_path = item.get('blobPath', None)

                if layer in ['silver', 'gold'] and not task_file_path.lower().endswith('.json'):
                    continue

                layer_args = {
                    "databricksMode": databricks_mode,
                    "cosmosConfName": cosmos_conf_name,
                    "secretScope": secret_scope,
                    "taskFilePath": task_file_path,
                    "configFolder": config_folder,
                    "Cur_Job_ID": "{{job_id}}",
                    "Cur_Run_Id": "{{run_id}}",
                    "Cur_Task_Name": "{{task_key}}"
                }

                workflow_tasks.append({
                    'name': task_name,
                    'args': [layer_args],
                    'layer': layer,
                    'blob_path': blob_path
                })

    job_schedule_config(workflow_tasks, uid, job_name)


def job_schedule_config(workflow_tasks, uid, job_name):
    print("the jobname", job_name)
    job_config = {
        "name": job_name,
        "tasks": [],
    }
    previous_task_key = None
    for task in workflow_tasks:
        task_name = task['name']
        blob_path = task['blob_path']
        layer = task['layer']
        task_args_list = task['args']
        args = task_args_list[0]
        subtask_info = {
            "task_key": task_name,
            "existing_cluster_id": existing_cluster_id,
            "spark_jar_task": {
                "main_class_name": main_class,
                "parameters": str(args)
                # "jar_url": jar_path
            },
            "libraries": [{"jar": jar_path}],
        }
        if previous_task_key:
            # Set dependency on the previous task
            subtask_info['depends_on'] = [{'task_key': previous_task_key}]
        job_config['tasks'].append(subtask_info)
        previous_task_key = task_name
        create_and_run_databricks_job(job_config, task_name, uid, blob_path, job_name)


def create_and_run_databricks_job(job_info, task_name, uid, blob_path, job_name):
    # Converting the config_data_json dictionary to a JSON string
    config_data_json = json.dumps(job_info)
    print("config_data_json", config_data_json)

    config_data_json = config_data_json.replace("'", '"').replace('"[{', "[").replace('}]"', "]")
    print("args info", config_data_json)

    jobid = read_databricks_job(job_name, uid)

    if jobid:
        # updating the existing job details
        print("job_id:", jobid, type(jobid))
        update_job_command = ["databricks", "jobs", "reset", "--job-id", str(jobid), "--json", config_data_json]
        print("running update job command:", update_job_command)

        response = subprocess.run(update_job_command, capture_output=True, text=True)
        if response.returncode != 0:
            print("stderr:", response.stderr)
            print("stdout:", response.stdout)
            print("std return:", response.returncode)
            raise Exception(f"Error updating the job: {response.stderr}")
        else:
            print("Job details updated successfully.")
            # Insert or update the job_id in the database
            insert_data_into_postdb(jobid, job_name, uid, blob_path, task_name)
    else:
        create_job_command = ["databricks", "jobs", "create", "--json", config_data_json]
        response = subprocess.run(create_job_command, capture_output=True, text=True)
        if response.returncode != 0:
            print("stderr:", response.stderr)
            print("stdout:", response.stdout)
            print("std return:", response.returncode)
            raise Exception(f"Error creating job: {response.stderr}")
        else:
            response_json = json.loads(response.stdout)
            job_id = response_json["job_id"]

            # Insert or update the job_id in the database
            insert_data_into_postdb(job_id, job_name, uid, blob_path, task_name)


def read_databricks_job(job_name, uid):
    workflow_db = connect_to_database(workflow_database)
    try:
        cursor = workflow_db.cursor()

        # SQL query from filtering the job_name & uid insert new JSON data
        query = sql.SQL("SELECT json_data['databricks']['job_id'] as jobid FROM {}"
                        "WHERE json_data->> 'uid' = {} and json_data->> 'jobname' = {}").format(
            sql.Identifier(workflow_container),
            sql.Literal(uid),
            sql.Literal(job_name)
        )
        print(query)
        cursor.execute(query)
        rows = cursor.fetchone()
        print("rows data", rows[0])

        if rows:
            jobid = rows[0]
            print(f"Item with job_id {uid}, job name {job_name} are available successfully.")
            return jobid
        else:
            print(f"No matching record found for job_id: {uid}, job name: {job_name}.")
            return None

    except psycopg2.Error as e:
        print(f"Error reading the workflow table . Error:{e}")


def insert_data_into_postdb(job_id, job_name, uid, blob_path, task_name):
    workflow_db = connect_to_database(workflow_database)
    new_item = {
        "databricks": {
            "job_id": job_id
        },
        'jobname': job_name,
        'uid': uid,
        'blob_path': blob_path,
        "jobid": str(job_id),
        "task_name": str(task_name)
    }
    try:
        cursor = workflow_db.cursor()
        # Converting python dictionary to a JSON string
        json_data_str = json.dumps(new_item)
        # SQL query to insert new JSON data if jobid exists it will update
        query = sql.SQL("""
            INSERT INTO {} (jobid, json_data)
            VALUES (%s, %s)
            ON CONFLICT (jobid) DO UPDATE
            SET json_data = EXCLUDED.json_data
        """).format(
            sql.Identifier(workflow_container)
        )
        # Executing the query with the JSON data string
        cursor.execute(query, (str(job_id), json_data_str))
        # Committing the changes
        workflow_db.commit()
        # Closing the cursor
        cursor.close()
        print(f"Item with job_id {job_id}, Job name {job_name} inserted successfully")
    except psycopg2.Error as e:
        print(f"Error inserting new JSON data into the table. Error: {e}")

















def create_and_run_databricks_job(job_info, task_name, uid, blob_path, job_name):
    host_url = None
    pat_token = None
    try:
        # connecting to the Databricks for retrieving token
        databricks_secret = connect_to_keyvault(key_vault_name, databricks_secret_name)
        if databricks_secret:
            host_url, pat_token = databricks_secret.split(',')

        # Converting the config_data_json dictionary to a JSON string
        config_data_json = json.dumps(job_info)
        print("config_data_json", config_data_json)

        config_data_json = config_data_json.replace("'", '"').replace('"[{', "[").replace('}]"', "]")
        print("args info", config_data_json)

        job_id = read_databricks_job(job_name, uid)

        headers = {
            'Authorization': f'Bearer {pat_token}',
            'Content-Type': 'application/json',
        }

        if job_id:
            # updating the existing job details
            print("job_id:", job_id, type(job_id))
            update_job_url = f'{host_url}/api/2.0/jobs/reset'
            update_job_payload = {
                'job_id': job_id,
                'new_settings': json.loads(config_data_json)
            }
            print("running update job URL:", update_job_url)

            response = requests.post(update_job_url, headers=headers, json=update_job_payload)
            response.raise_for_status()
            response_json = response.json()

            if 'error_code' in response_json:
                raise Exception(f"Error updating the job: {response_json['error_code']}: {response_json['message']}")

            print("The Job details updated successfully.")
            # Updating the job_id details in the database
            insert_data_into_postdb(job_id, job_name, uid, blob_path, task_name)
        else:
            create_job_url = f'{host_url}/api/2.0/jobs/create'
            create_job_payload = {
                'name': job_name,
                'new_settings': json.loads(config_data_json)
            }

            response = requests.post(create_job_url, headers=headers, json=create_job_payload)
            response.raise_for_status()  # Raise an exception for HTTP errors
            response_json = response.json()

            if 'error_code' in response_json:
                raise Exception(f"Error creating job: {response_json['error_code']}: {response_json['message']}")

            job_id = response_json["job_id"]

            # Insert or update the job_id in the database
            insert_data_into_postdb(job_id, job_name, uid, blob_path, task_name)





