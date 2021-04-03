from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import json
import csv 
import requests



def download_rates():
    with open('C:/Users/asaph/OneDrive/Documentos/GitHub/airflow-classes/airflow-materials/airflow-section-3/mnt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://v6.exchangerate-api.com/v6/ab559bcaff640f3a60259842/latest/' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['time_last_update_utc']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['conversion_rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')



default_args = {
    "owner":"Asaph Tinoco",
    "start_date": datetime(2020,3,31),
    "depends_on_past":False,
    "email_on_failure":False, 
    "email_on_retry":False,
    "email":"asaphd.tinoco@poli.ufrj.br",
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id = "forex_data_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False) as dag: 

    is_forex_rates_available = HttpSensor(
        task_id='Checking HTTPS Availibility',
        method='GEY',
        http_conn_id='forex_api',
        endpoint="latest",
        response_check=lambda response: "rates" in response.txt, 
        poke_interval=5,
        timeout=20
        )

    download_rates = PythonOperator(
        task_id = 'Downloading Rates',
        python_callable = download_rates
    )   


    saving_rates = BashOperator(
        task_id='Saving Rates'
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW/dags/files/forex_rates.json /forex
        """
    )