def create_and_run_databricks_job(job_info, job_name, uid, blob_path):
    # Converting the config_data_json dictionary to a JSON string
    config_data_json = json.dumps(job_info)
    print("config_data_json", config_data_json)

    config_data_json = config_data_json.replace("'", '"').replace('"[{', "[").replace('}]"', "]")
    print("args info", config_data_json)

    job_id = read_databricks_job(job_name, uid)

    if job_id:
        # updating the existing job details
        print("job_id:", job_id, type(job_id))
        update_job_command = ["databricks", "jobs", "reset", "--job-id", str(job_id), "--json", config_data_json]
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
            insert_data_into_postdb(job_id, job_name, uid, blob_path)
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
            insert_data_into_postdb(job_id, job_name, uid, blob_path)



def create_and_run_databricks_job(job_info, job_name, uid, blob_path):
    # Converting the config_data_json dictionary to a JSON string
    config_data_json = json.dumps(job_info)
    print("config_data_json", config_data_json)

    config_data_json = config_data_json.replace("'", '"').replace('"[{', "[").replace('}]"', "]")
    print("args info", config_data_json)

    job_id = read_databricks_job(job_name, uid)

    if job_id:
        # Updating the existing job details
        print("job_id:", job_id, type(job_id))
        update_job_command = ["databricks", "jobs", "reset", "--job-id", str(job_id), "--json", config_data_json]
        print("running update job command:", update_job_command)

        # Using subprocess.Popen for more flexibility
        try:
            process = subprocess.Popen(update_job_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                print("stderr:", stderr.decode())
                print("stdout:", stdout.decode())
                raise Exception(f"Error updating job: {stderr.decode()}")
            else:
                print("Job details updated successfully.")
                # Insert or update the job_id in the database
                insert_data_into_postdb(job_id, job_name, uid)

        except Exception as e:
            print(f"Error updating job: {e}")
    else:
        create_job_command = ["databricks", "jobs", "create", "--json", config_data_json]
        response = subprocess.run(create_job_command, capture_output=True, text=True)

        if response.returncode != 0:
            print("stderr:", response.stderr)
            print("stdout:", response.stdout)
            print("std return:", response.returncode)
            raise Exception(f"Error creating job: {response.stderr}")
        else:
            # the job ID from the response
            response_json = json.loads(response.stdout)
            job_id = response_json["job_id"]

            # Insert or update the job_id in the database
            insert_data_into_postdb(job_id, job_name, uid)


def insert_data_into_postdb(job_id, job_name, uid, blob_path):
    workflow_db = connect_to_database(workflow_database)
    new_item = {
        "databricks": {
            "job_id": job_id
        },
        'jobname': job_name,
        'uid': uid,
        'blob_path': blob_path,
        "jobid": str(job_id)
    }
    try:
        cursor = workflow_db.cursor()
        # Converting python dictionary to a JSON string
        json_data_str = json.dumps(new_item)
        # insert new JSON data if jobid exists it will update
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
