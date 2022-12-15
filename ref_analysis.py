from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("get uf").getOrCreate()

df = spark.read.parquet('s3://ref-layer-bucket-xpto/tweeter/sa-east-1/tweeter_eleicoes/year=2022/')

df = df.withColumn('pontuacao', col("pontuacao").cast(FloatType()))

df = df.where(df.uf != 'erro')

df = df.groupBy(['uf','candidato','sentimento']).agg(count('sentimento').alias('contador'),avg('pontuacao').alias('media'))

df = df.withColumn('year', lit(2022))

df.write.mode("append").partitionBy("candidato","uf",'sentimento') \
    .parquet('s3://trusted-layer-bucket-xpto/tweeter/sa-east-1/tweeter_analise/')