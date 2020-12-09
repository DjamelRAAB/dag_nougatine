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

    # install = BashOperator(
    #     task_id = "install",
    #     bash_command = "pip install /root/airflow/dags/dag_nougatine/.",
    # ) 

    # mkdir = BashOperator(
    #     task_id = "mkdir",
    #     bash_command = "mkdir /tmp/data_groupe6",
    # ) 

    # get_data = PythonOperator(
    #     task_id='get_data',
    #     python_callable=get_data.launch,
    #     op_kwargs={
    #         'path':"/tmp/data_groupe6"
    #     }
    # )

    # clean = BashOperator(
    #     task_id = "clean",
    #     bash_command = "rm -rf /tmp/data_groupe6",
    # )
    
    # mkdir_dist = BashOperator(
    #     task_id = "mkdir_dist",
    #     bash_command = "hdfs dfs -mkdir /user/iabd2_group6/data/{}".format(current_time),
    # ) 

    # put_data_to_hdfs = BashOperator(
    #     task_id = "put_data_to_hdfs",
    #     bash_command = "hdfs dfs -moveFromLocal /tmp/data_groupe6/*.csv /user/iabd2_group6/data/{}".format(current_time),
    # ) 

    # put_src_to_hdfs = BashOperator(
    #     task_id = "put_src_to_hdfs",
    #     bash_command = "hdfs dfs -rm -skipTrash  /user/iabd2_group6/app/*.py && hdfs dfs -moveFromLocal /root/airflow/dags/dag_nougatine/src_app/*.py /user/iabd2_group6/app/",
    # ) 

    submit_t1 = BashOperator(
        task_id = "submit_t1",
        bash_command = "export HADOOP_CONF_DIR=/etc/hadoop/conf && spark-submit --master yarn --deploy-mode cluster hdfs://d271ee89-3c06-4d40-b9d6-d3c1d65feb57.priv.instances.scw.cloud:8020/user/iabd2_group6/app/collect_from_spotify.py",
    ) 

#[install, mkdir] >> get_data >> mkdir_dist >> [put_data_to_hdfs, put_src_to_hdfs] >> clean >> submit_t1 

