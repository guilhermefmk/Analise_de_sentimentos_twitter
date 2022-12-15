from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("get uf").getOrCreate()

df = spark.read.parquet('s3://ref-layer-bucket-xpto/tse/')

df = df.withColumn('nm_candidato', 
                when(df.nm_candidato == 'JAIR MESSIAS BOLSONARO',regexp_replace('nm_candidato','JAIR MESSIAS BOLSONARO','Bolsonaro')) \
                .when(df.nm_candidato == 'LUIZ INACIO LULA DA SILVA',regexp_replace('nm_candidato','LUIZ INACIO LULA DA SILVA','Lula'))    
                )

df = df.drop_duplicates()

percent_uf = Window.partitionBy("sg_uf").orderBy(col("porcentagem").desc())

df = df.withColumn("row",row_number().over(percent_uf)) \
  .filter(col("row") == 1).drop("row")


df.write.mode("append").partitionBy("year","nm_candidato","sg_uf") \
    .parquet('s3://trusted-layer-bucket-xpto/tse/sa-east-1/')