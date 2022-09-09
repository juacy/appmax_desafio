from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'juacy',
    'start_date': datetime(2022, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG('spark_ip_silver', description = 'Tratando os dados de IP da bronze e salvando na camada silver', catchup = False, schedule_interval = "@once", default_args = default_args)


s1 = SparkSubmitOperator(
    task_id = "ip-silver",
    application = "./dags/atividade_2.py",
    conn_id = "conexao_spark",
    dag = dag
)
