from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'juacy',
    'start_date': datetime(2022, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG('spark_pedidos_silver', description = 'Tratando os dados de pedido da camada bronze e salvando na silver', catchup = False, schedule_interval = "@once", default_args = default_args)

s1 = SparkSubmitOperator(
    task_id = "pedidos-silver",
    application = "./dags/atividade_1.py",
    conn_id = "conexao_spark",
    dag = dag
)