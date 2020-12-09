from airflow import DAG 
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def hello_world(arg1): 
    print("Hello {} by BashOperator".format(arg1))

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

    python_hello_world = PythonOperator(
        task_id='python_hello_world',
        python_callable=hello_world,
        op_kwargs={
            'arg1'          : "Group6"
        }
    )
    
echo_hello_world >> python_hello_world
