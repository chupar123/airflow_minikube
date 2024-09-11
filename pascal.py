from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def PrintPasTriangle():
    row = [1]
    for i in range(10):
        print(row)
        row = [sum(x) for x in zip([0] + row, row + [0])]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 11, 8, 44, 0),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'print_pas_triangle_dag',
    default_args=default_args,
    description='print pascal triangle',
    schedule_interval='@daily',
)

t1 = PythonOperator(
    task_id='PrintPasTriangle',
    python_callable=PrintPasTriangle,
    dag=dag,
)
