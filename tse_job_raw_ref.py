from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder.appName("Refine tse 2022").getOrCreate()

df = spark.read.csv('s3://raw-layer-bucket-xpto/tse/sa-east-1/year=2022/2022.csv', header=True, inferSchema=True,sep=';')

total = df.groupBy('sg_uf').sum('qt_votos_nominais').withColumnRenamed("sum(qt_votos_nominais)", "total_votos").sort('sg_uf')
total = total.withColumnRenamed("sg_uf", "uf_drop")

df = df.join(total, df.sg_uf == total.uf_drop,'left').drop('uf_drop','ds_cargo','dt_carga','nr_turno')

df = df.withColumn('porcentagem', (col('qt_votos_nominais') * 100)/col('total_votos')).withColumn('year', lit(2022)).withColumn('turno', lit(2))

df.write.mode("append").partitionBy("year","turno","sg_uf",'nm_candidato') \
    .parquet('s3://ref-layer-bucket-xpto/tse/sa-east-1/')