import json
import tweepy
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
import boto3
from datetime import datetime

BEARER_TOKEN = "YOUR TWITTER KEY HERE"

class GetTweets(tweepy.StreamingClient):
    contador = 0
    
    listajson = []
    s3 = boto3.resource(
        's3',
        region_name='sa-east-1',
        aws_access_key_id='YOUR AWS KEY HERE',
        aws_secret_access_key='YOUR AWS KEY HERE'
    )
    def on_data(self, raw_data):
        raw_data = json.loads(raw_data.decode('utf-8'))
        if 'includes' in raw_data:
            self.contador += 1
            self.master += 1
            data = datetime.strftime(datetime.strptime(raw_data['data']['created_at'],'%Y-%m-%dT%H:%M:%S.%f%z'), '%Y-%m-%d %H:%M:%S')
            text = raw_data['data']['text']
            id = raw_data['data']['id']
            geo = raw_data['includes']['places'][0]['full_name']
            tjson = {
                "id":id,
                "tweet_text":text,
                "tweet_date":data,
                "geo": geo,
                "candidato": 'Bolsonaro'
            }
            self.listajson.append(tjson)
            if self.contador >=100:
                name = data
                self.s3.Object('raw-layer-bucket-xpto', f'tweeter/sa-east-1/tweeter_eleicoes/2022/{name}.json').put(Body=json.dumps(self.listajson,ensure_ascii=False))
                self.contador = 0
                self.listajson = []
        return True



    def on_connect(self):
        print('connected')


        
keyword = "Bolsonaro"
printer = GetTweets(bearer_token=BEARER_TOKEN)
printer.add_rules(tweepy.StreamRule(keyword))
printer.filter(tweet_fields=['geo','created_at','text','id'],expansions='geo.place_id')