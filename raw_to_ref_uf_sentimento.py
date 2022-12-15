from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from geopy.geocoders import Photon
from geopy.extra.rate_limiter import RateLimiter
from time import sleep
from geopy.exc import GeocoderUnavailable
from datetime import datetime

def get_uf(cidade):
    states = {'Acre','Alagoas','Amapá','Amazonas','Bahia','Ceará','Distrito Federal',
            'Espírito Santo','Goiás','Maranhão','Mato Grosso','Mato Grosso do Sul',
            'Minas Gerais','Pará','Paraíba','Paraná','Pernambuco','Piauí',
            'Rio de Janeiro','Rio Grande do Norte','Rio Grande do Sul','Rondônia',
            'Roraima','Santa Catarina','São Paulo','Sergipe','Tocantins'
            }
    sleep(1.1)
    attempt=1
    max_attempts=5
    try:
        geolocator = Photon()
        locate = RateLimiter(geolocator.geocode, min_delay_seconds=1)
        adress = locate(cidade)
        uf = set(adress[:-1][0].split(', '))
        if states.intersection(uf):
            attempt=0
            return list(states.intersection(uf))[0]
        else:
            attempt=0
            return 'erro'
    except GeocoderUnavailable:
        if attempt <= max_attempts:
            attempt+=1
            return get_uf(cidade)
        raise

def format_date(date):
    if date.find(':') != -1:
        result = datetime.strftime(datetime.strptime(date,'%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    else:
        result = datetime.strftime(datetime.strptime(date,'%Y-%m-%d %H-%M-%S'), '%Y-%m-%d %H:%M:%S')
    return result

def analyser(text):
    import requests
    import nltk
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    nltk.download("vader_lexicon")
    link = 'https://raw.githubusercontent.com/rafjaa/LeIA/master/lexicons/vader_lexicon_ptbr.txt'
    req = requests.get(link)
    req = req.text
    lista = req.split('\n')
    lista.pop()
    dicionario = {}
    for linha in lista:
        linha = linha.split('\t')
        dicionario[linha[0]]= float(linha[1])
    sent_analyzer = SentimentIntensityAnalyzer()
    sent_analyzer.lexicon.update(dicionario)
    result_dict = sent_analyzer.polarity_scores(text)
    return result_dict['compound']

spark = SparkSession.builder.appName("uf and sentiment").getOrCreate()

df = spark.read.json('s3://raw-layer-bucket-xpto/tweeter/sa-east-1/tweeter_eleicoes/2022', multiLine=True)

analyze = udf(analyser, FloatType())
getuf = udf(get_uf, StringType())
getdate = udf(format_date, StringType())

df = df.select('*').withColumn(
                'tweet_text',lower('tweet_text')
        ).withColumn(
            'sentimento', when(
                analyze('tweet_text') > 0 , 'positivo'
            ).when(
                analyze('tweet_text') < 0 , 'negativo'
            ).when(
                analyze('tweet_text') == 0 , 'neutro'
            )
        ).withColumn('pontuacao',
            when(
            col('sentimento') == 'neutro', '0' 
            ).otherwise(
                analyze('tweet_text')
            )
        ).withColumn(
            'cidade', split(col('geo'),', ')[0]
        ).withColumn(
            'pais', split(col('geo'),', ')[1])
            

df = df.where(col('pais').isNotNull())

df = df.withColumn("tweet_date", to_timestamp(getdate('tweet_date')))

df = df.withColumn("year", date_format(col("tweet_date"), "yyyy")) \
            .withColumn("month", date_format(col("tweet_date"), "MM")) \
            .withColumn("day", date_format(col("tweet_date"), "dd")) \
            .withColumn('tweet_text', regexp_replace('tweet_text','\n',' '))

df = df.where((df.pais == 'Brasil') | (df.pais == 'Brazil'))

df = df.withColumn('uf', getuf(col('geo')))

df.write.mode("append").partitionBy("year",'candidato', "month", "day", "uf") \
    .parquet('s3://ref-layer-bucket-xpto/tweeter/sa-east-1/tweeter_eleicoes/')