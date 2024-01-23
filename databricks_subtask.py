def job_schedule_config(workflow_tasks, uid):
    job_config = {
        "name": f"Workflow_{uid}",
        "existing_cluster_id": existing_cluster_id,
        "libraries": [{"jar_url": jar_path}],
        'tasks': []
    }

    for task in workflow_tasks:
        task_name = task['name']
        layer = task['layer']
        task_args_list = task['args']

        
        task_args = task_args_list[0]

        subtask_info = {
            'task_key': task_name,
            'notebook_task': {
                'notebook_path': task_args.get('taskFilePath', ''),  
                'base_parameters': {
                    'databricksMode': task_args.get('databricksMode', ''),
                    'cosmosConfName': task_args.get('cosmosConfName', ''),
                    'secretScope': task_args.get('secretScope', ''),
                    'configFolder': task_args.get('configFolder', ''),
                    # Add more parameters as needed
                    'param1': 'value1',
                    'param2': 'value2'
                }
            }
        }

        job_config['tasks'].append(subtask_info)
