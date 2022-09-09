from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import requests
from pyspark.sql.types import DoubleType,TimestampType, StructField, StringType, StructType

spark = SparkSession.builder.getOrCreate()

pedidos = spark.read.parquet("file:///opt/airflow/dags/spark_jobs/dados/silver/pedidos.parquet")

ips = spark.read.parquet("file:///opt/airflow/dags/spark_jobs/dados/silver/api.parquet")

pedidos.show()

ips.show()

ips = ips.withColumnRenamed("id","id_ip_api")

tabela = pedidos.join(ips, pedidos.id_ip == ips.id_ip_api, how='left')

tabela.printSchema()

tabela_oficial = tabela.select(['id', 'nome', 'sobrenome', 'email', 'valor_pedido','data_pedido','tipo_cc','nome completo', 'regiao','cidade', 'pais'])

tabela_oficial.write.parquet("file:///opt/airflow/dags/spark_jobs/dados/gold/pedidos_com_ip.parquet")

