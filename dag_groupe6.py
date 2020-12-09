from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from apispotify import get_data


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
    #     bash_command = "pip install /usr/local/airflow/dags/dag_nougatine/.",
    # ) 

    mkdir = BashOperator(
        task_id = "mkdir",
        bash_command = "mkdir /tmp/data_groupe6",
    ) 
    
    # get_data = PythonOperator(
    #     task_id='get_data',
    #     python_callable=get_data.launch,
    #     op_kwargs={
    #         'path':"/usr/local/airflow/dags/dag_nougatine/data"
    #     }
    #)
    
#git echo_hello_world >>  install >> python_hello_world >> 
mkdir

