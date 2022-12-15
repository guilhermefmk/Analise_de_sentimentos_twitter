from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("get uf").getOrCreate()


sent = spark.read.parquet('s3://ref-layer-bucket-xpto/tweeter/sa-east-1/tweeter_eleicoes/year=2022/')

teste = sent.groupBy(['candidato','uf','sentimento']).agg(count('sentimento')).withColumnRenamed("count(sentimento)", "total_sentimento")

temp = teste.groupBy(['candidato','uf']).agg(sum('total_sentimento')).withColumnRenamed('sum(total_sentimento)','total_uf')
temp = temp.withColumnRenamed("uf", "uf_drop").withColumnRenamed('candidato','candidato_drop')

final = teste.join(temp, [teste.uf == temp.uf_drop, teste.candidato == temp.candidato_drop],'left').drop('uf_drop','candidato_drop')

final = final.withColumn('porcentagem', (col('total_sentimento') * 100)/col('total_uf'))


percent_uf = Window.partitionBy("uf").orderBy(col("porcentagem").desc())


final = final.where(final.sentimento == 'negativo')


final = final.withColumn("row",row_number().over(percent_uf)) \
  .filter(col("row") == 1).drop("row")


final.write.mode("append").partitionBy("year","nm_candidato","sg_uf") \
    .parquet('s3://trusted-layer-bucket-xpto/tweeter/sa-east-1/tweeter_winners/')