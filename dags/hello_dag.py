try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print("Todas as bibliotecas estão ok...")
except Exception as er:
    print("Error {} ".format(er))

def first_function_execution(**context):
    print("first_function_execution")
    context['ti'].xcom_push(key='mykey', value='first_function_execution says hello')

def second_function_execution(**context):
    instance = context.get('ti').xcom_pull(key = 'mykey')
    print('Second function executed {}'.format(instance))

with DAG(
        dag_id = 'hello_dag',
        schedule_interval = '@daily',
        default_args = {
            'owner' : 'airflow',
            'retries' : 1,
            'retry_delay' : timedelta(minutes=5),
            'start_date' : datetime(2022, 1, 1),
        },
        catchup = False) as f:

    first_function_execution = PythonOperator(
        task_id = 'first_function_execution',
        provide_context = True,
        python_callable=first_function_execution,
        op_kwargs={"name":"Nardi Lucas"},
    )

    second_function_execution = PythonOperator(
        task_id = 'second_function_execution',
        provide_context = True,
        python_callable=second_function_execution,
        op_kwargs={"name":"Nardi Lucas"},
    )

    first_function_execution >> second_function_execution