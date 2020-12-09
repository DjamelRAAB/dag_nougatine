from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from apispotify import get_data


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
    echo_hello_world = BashOperator(
        task_id = "echo_hello_world",
        bash_command = "echo Hello groupe6 by BashOperator",
    )
    
    # venv = BashOperator(
    #     task_id = "venv",
    #     bash_command = "python -m venv /usr/local/airflow/dags/dag_nougatine/python-venv && mkdir /usr/local/airflow/dags/dag_nougatine/data ",
    # )    

    # activate = BashOperator(
    #     task_id = "activate",
    #     bash_command = "source /usr/local/airflow/dags/dag_nougatine/python-venv/bin/activate",
    # ) 

    install = BashOperator(
        task_id = "install",
        bash_command = "pip install /usr/local/airflow/dags/dag_nougatine/.",
    ) 

    # deactivate = BashOperator(
    #     task_id = "deactivate",
    #     bash_command = "deactivate",
    # ) 

    # ls_data = BashOperator(
    #     task_id = "ls_data",
    #     bash_command = "/usr/local/airflow/dags/dag_nougatine/data",
    # )


    python_hello_world = PythonOperator(
        task_id='python_hello_world',
        python_callable=hello_world,
        op_kwargs={
            'arg1': "Group6"
        }
    )
    
    # get_data = PythonOperator(
    #     task_id='get_data',
    #     python_callable=get_data.launch,
    #     op_kwargs={
    #         'path':"/usr/local/airflow/dags/dag_nougatine/data"
    #     }
    # )
    
echo_hello_world >>  install >> python_hello_world

