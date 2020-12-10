import datetime as dt 
from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apispotify import get_data

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

    submit_t1 = BashOperator(
        task_id = "submit_t1",
        bash_command = "export HADOOP_CONF_DIR=/etc/hadoop/conf && export HADOOP_USER_NAME=iabd2_group6 && spark-submit --master yarn --deploy-mode cluster hdfs://d271ee89-3c06-4d40-b9d6-d3c1d65feb57.priv.instances.scw.cloud:8020/user/iabd2_group6/app/load_data_into_hive.py",
    ) 

submit_t1 

