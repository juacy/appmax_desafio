from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import requests
from pyspark.sql.types import DoubleType,TimestampType, StructField, StringType, StructType

spark = SparkSession.builder.getOrCreate()

ips = spark.read.format("csv").option("header","true").load("file:///opt/airflow/dags/spark_jobs/dados/bronze/ips.csv")

with open("//opt/airflow/dags/spark_jobs/dados/bronze/ips_api.csv",'w',encoding = 'utf-8') as file:
    file.write('ip_api,cidade,regiao,pais\n')

for row in ips.rdd.collect():
    try:
        response = requests.get("http://ipwho.is/{ip}".format(ip=row['ip']))
        with open("//opt/airflow/dags/spark_jobs/dados/bronze/ips_api.csv",'a',encoding = 'utf-8') as file:
            file.write('{ip},{cidade},{regiao},{pais}\n'.format(ip=response.json()['ip'], cidade=response.json()['city'], pais=response.json()['country'], regiao=response.json()['region']))
    except:
        pass

ips_api = spark.read.format("csv").option("header","true").load("file:///opt/airflow/dags/spark_jobs/dados/bronze/ips_api.csv")

tabela_oficial = ips.join(ips_api, ips.ip == ips_api.ip_api, how='left')

tabela_oficial = tabela_oficial.select(['id', 'ip', 'user_agent', 'regiao','cidade', 'pais'])

tabela_oficial.write.parquet("file:///opt/airflow/dags/spark_jobs/dados/silver/api.parquet")