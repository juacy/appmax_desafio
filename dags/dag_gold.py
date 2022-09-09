from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'juacy',
    'start_date': datetime(2022, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG('spark_pedidos_ip_gold', description = 'Agrupando os dados de pedidos e IP', catchup = False, schedule_interval = "@once", default_args = default_args)

s1 = SparkSubmitOperator(
    task_id = "pedidos_ip-gold",
    application = "./dags/atividade_3.py",
    conn_id = "conexao_spark",
    dag = dag
)