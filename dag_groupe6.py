from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apispotify import get_data
import datetime as dt 

now = dt.datetime.now() #- dt.timedelta(days=7)
current_time = now.strftime('%Y%m%d')

def hello_world(arg1): 
    print("Hello {} by PythonOperator".format(arg1))

default_args = {
    'owner':"Group6",
    'start_date': datetime(2020, 12, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False
}

# Create dag
dag = DAG ( 'dag_groupe6',
            default_args=default_args, 
            schedule_interval='45 12 * * *'
            )

with dag:

    install = BashOperator(
        task_id = "install",
        bash_command = "pip install /root/airflow/dags/dag_nougatine/.",
    ) 

    mkdir = BashOperator(
        task_id = "mkdir",
        bash_command = "mkdir /tmp/data_groupe6",
    ) 

    clean = BashOperator(
        task_id = "clean",
        bash_command = "rm -rf /tmp/data_groupe6",
    )
    
    mkdir_dist = BashOperator(
        task_id = "mkdir_dist",
        bash_command = "hdfs dfs -mkdir /user/iabd2_group6/data/{}".format(current_time),
    ) 

    put_to_hdfs = BashOperator(
        task_id = "put_to_hdfs",
        bash_command = "hdfs dfs -moveFromLocal /tmp/data_groupe6/*.csv /user/iabd2_group6/data/{}".format(current_time),
    ) 

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data.launch,
        op_kwargs={
            'path':"/tmp/data_groupe6"
        }
    )
    

[install, mkdir] >> get_data >> mkdir_dist >> put_to_hdfs >> clean