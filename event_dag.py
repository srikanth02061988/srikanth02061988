from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.configuration import conf
from pathlib import Path

project_id = conf.get('webserver', 'project_id')
region = conf.get('webserver', 'region')


with DAG
        dag_id ='dag_event_gird_incr_files_extract',
        default_args ={
            'depends_on_past' : False,
            'email_on_failure': True,
            'email_on_retry'  : False
        },
        description='This Dag will fetch data from Event gird'
        schedule_interval=None
        catchup=False,
        max_active_runs=1,
        start_date=datetime(2023,4,5),
        tags=['Event Gird data extract'],
) as dag:
    # This operator does nothing. 
    start_task = EmptyOperator(
        task_id='start_task', # The name of the sub-task in the workflow.
        dag=dag # When using the "with Dag(...)"
    )
    
    

    # With the PythonOperator you can run a python function.
    extract_event_data = PythonOperator(
        task_id='extract_event_data',
        python_callable=main,
        dag=dag
    )
    
        
    end_task = PythonOperator(
        task_id="end_task",
        dag=dag
    )


    # Define the order in which the tasks are supposed to run
    start_task >> extract_event_data >> end_task 