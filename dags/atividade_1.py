from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType,TimestampType

spark = SparkSession.builder.getOrCreate()

pedidos = spark.read.format("csv").option("header","true").load("file:///opt/airflow/dags/spark_jobs/dados/bronze/pedidos.csv")
pedidos.printSchema()

pedidos = pedidos.withColumn('nome completo', F.concat(F.col('nome'), F.lit(" "), F.col('sobrenome')))

pedidos = pedidos.withColumn('valor_pedido', F.translate('valor_pedido', '$', ''))

pedidos = pedidos.withColumn('valor_pedido', F.col('valor_pedido').cast(DoubleType())) \
  .withColumn('data_pedido', F.to_timestamp(F.col("data_pedido"),"yyyy-MM-dd HH:mm:ss"))

pedidos.printSchema()
pedidos.write.parquet("file:///opt/airflow/dags/spark_jobs/dados/silver/pedidos.parquet")