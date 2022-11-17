# -*- coding: utf-8 -*-

import os
import tweepy
from dotenv import load_dotenv
import requests
import pandas as pd
import psycopg2

load_dotenv("keys.env",override=True)

consumer_key = os.environ["API_KEY"]
consumer_secret = os.environ["API_KEY_SECRET"]
access_token = os.environ["ACCESS_TOKEN"]
access_token_secret = os.environ["ACCESS_TOKEN_SECRET"]
bearer_token = os.environ["BEARER_TOKEN"]

database_pg = os.environ["DATABASE"]
user_pg = os.environ['USER']
password_pg = os.environ['PASSWORD']
host_pg = os.environ['HOST']
port_pg = os.environ['PORT']

client = tweepy.Client( bearer_token=bearer_token, 
                        consumer_key=consumer_key, 
                        consumer_secret=consumer_secret, 
                        access_token=access_token, 
                        access_token_secret=access_token_secret, 
                        return_type = requests.Response,
                        wait_on_rate_limit=True)

query = 'from:FlaviaAngelim'

# get max. 100 tweets
tweets = client.search_recent_tweets(query=query, 
                                    tweet_fields=['author_id', 'created_at'],
                                     max_results=100)


# Save data as dictionary
tweets_dict = tweets.json() 

# Extract "data" value from dictionary
tweets_data = tweets_dict['data'] 

# Transform to pandas Dataframe
df = pd.json_normalize(tweets_data)

# Select Data From Twitter
df = df[['id','text','created_at','author_id']]

# Convert Types
df['id'] = df['id'].astype(float)
df['text'] = df['text'].astype(str)
df['created_at'] = pd.to_datetime(df['created_at'], infer_datetime_format=True)
df['author_id'] = df['author_id'].astype(int)

# Connect With Postgre

conn = psycopg2.connect(
   database=database_pg, 
   user= user_pg, 
   password= password_pg, 
   host= host_pg, 
   port= port_pg
)

def update_data(conection,data):
    #Setting auto commit false
    conection.autocommit = True
    
    #Creating a cursor object using the cursor() method
    cursor = conection.cursor()
    
    tuple_of_tuples = tuple([(tuple(x)) for x in data.values])
    
    query = """
        INSERT INTO TWEETS VALUES ("""+ str('%s,'*data.shape[1])[:-1] + """)"""
    
    cursor.executemany(query,tuple_of_tuples)
    print("Update Twitter Data")
    return
  
#update_data(conn,df)
