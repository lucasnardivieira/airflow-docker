from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.settings import AIRFLOW_HOME
import pandas as pd
import scripts.b2wScrap_script as b2wScrap
import scripts.magaluScrap_script as magaluScrap
import scripts.mercadoLivreScrap_script as mercadoLivreScrap


def task_inicial(ti):
    df = pd.read_csv(AIRFLOW_HOME + '/dags/data/lista_produtos.csv', usecols = ['store', 'url'])
    print(df)
    americanas_url_list = []
    magalu_url_list = []
    mercadoLivre_url_list = []
    for index, row in df.iterrows():
        if(row['store'] == 'americanas'):
            americanas_url_list.append(row['url'])
        elif(row['store'] == 'magalu'):
            magalu_url_list.append(row['url'])
        elif(row['store'] == 'mercadolivre'):
            mercadoLivre_url_list.append(row['url'])
        else:
            print('Não há task para a loja')
    
    ti.xcom_push(key='americanas_url_list', value=americanas_url_list) #MANDANDO PARAMETRO PARA TASKS DA AMERICANAS
    ti.xcom_push(key='magalu_url_list', value=magalu_url_list) #MANDANDO PARAMETRO PARA TASKS DA MAGALU
    ti.xcom_push(key='mercadoLivre_url_list', value=mercadoLivre_url_list) #MANDANDO PARAMETRO PARA TASK DO MERCADO LIVRE

def task_1_americanas(ti):
    url_list = ti.xcom_pull(key='americanas_url_list', task_ids='task_inicial')
    print(url_list)
    scrapped_product_list = []
    for url in url_list:
        scrapped_product = b2wScrap.acessa_site(url)
        scrapped_product_list.append(scrapped_product)
    ti.xcom_push(key='scrapped_product_list', value=scrapped_product_list) #MANDANDO PARAMETRO PARA OUTRA TASK
def task_2_americanas(ti):
    scrapped_product = ti.xcom_pull(key='scrapped_product_list', task_ids='task_1_americanas') #RECEBENDO PARAMETRO DE OUTRA TASK
    b2wScrap.convert_to_df(scrapped_product)

def task_1_magalu(ti):
    url_list = ti.xcom_pull(key='magalu_url_list', task_ids='task_inicial')
    print(url_list)
    scrapped_product_list = []
    for url in url_list:
        scrapped_product = magaluScrap.acessa_site(url)
        scrapped_product_list.append(scrapped_product)
    ti.xcom_push(key='scrapped_product_list', value=scrapped_product_list) #MANDANDO PARAMETRO PARA OUTRA TASK
def task_2_magalu(ti):
    scrapped_product = ti.xcom_pull(key='scrapped_product_list', task_ids='task_1_magalu') #RECEBENDO PARAMETRO DE OUTRA TASK
    magaluScrap.convert_to_df(scrapped_product)

def task_1_mercadoLivre(ti):
    url_list = ti.xcom_pull(key='mercadoLivre_url_list', task_ids='task_inicial')
    print(url_list)
    scrapped_product_list = []
    for url in url_list:
        scrapped_product = mercadoLivreScrap.acessa_site(url)
        scrapped_product_list.append(scrapped_product)
    ti.xcom_push(key='scrapped_product_list', value=scrapped_product_list) #MANDANDO PARAMETRO PARA OUTRA TASK
def task_2_mercadoLivre(ti):
    scrapped_product = ti.xcom_pull(key='scrapped_product_list', task_ids='task_1_mercadoLivre') #RECEBENDO PARAMETRO DE OUTRA TASK
    mercadoLivreScrap.convert_to_df(scrapped_product)

with DAG(
    dag_id='Scrap_dag',
    default_args={
        'owner':'airflow',
        'depends_on_past':False,
        'email':['airflow@airflow.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2022, 1, 1),
        #'end_date': datetime(2016, 1, 1),
    },
    description='Dag de scrap',
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2022, 1, 1),
    catchup=False,
        ) as dag:
    ######
    task_inicial = PythonOperator(
        task_id='task_inicial',
        python_callable=task_inicial,
        dag=dag,)
    ######
    task_1_americanas = PythonOperator(
        task_id='task_1_americanas',
        python_callable=task_1_americanas,
        dag=dag,)
    task_2_americanas= PythonOperator(
        task_id='task_2_americanas',
        python_callable=task_2_americanas,
        dag=dag,)
    ######
    task_1_magalu = PythonOperator(
    task_id='task_1_magalu',
    python_callable=task_1_magalu,
    dag=dag,)
    task_2_magalu = PythonOperator(
        task_id='task_2_magalu',
        python_callable=task_2_magalu,
        dag=dag,)
    ######
    task_1_mercadoLivre = PythonOperator(
        task_id='task_1_mercadoLivre',
        python_callable=task_1_mercadoLivre,
        dag=dag,)
    task_2_mercadoLivre = PythonOperator(
        task_id='task_2_mercadoLivre',
        python_callable=task_2_mercadoLivre,
        dag=dag,)

task_inicial >> task_1_americanas >> task_2_americanas
task_inicial >> task_1_magalu >> task_2_magalu
task_inicial >> task_1_mercadoLivre >> task_2_mercadoLivre