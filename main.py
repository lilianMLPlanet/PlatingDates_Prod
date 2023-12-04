# imports
import platform
import logging
import sys
import json
import os
import pymongo
import sys
import boto3
from pymongo import UpdateOne, InsertOne
import datetime
import numpy as np
import zipfile
import io
import geopandas as gpd
import time
from sklearn.neighbors import NearestNeighbors
import pandas as pd
from io import StringIO
import lightgbm as lgb
from io import BytesIO


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger('planting dates ')
logger.setLevel(logging.DEBUG)

eps = 1e-12


def get_gee_data(s3):
    # TODO: Dan MLmodel.getgee & store S3 BUCKET

def get_db_data(query, db_name, collection_name): 
    # TODO
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[db_name]
    mycol = mydb[collection_name]
    myquery = { query }
    mydoc = mycol.find(myquery) 
    df = pd.read_json(mydoc)
    return df

def get_s3_data(s3,bucket_name, object_key):
  
    obj = s3.get_object(Bucket=bucket_name, object_key)
    data = obj['Body'].read()  
    df = pd.read_csv(StringIO(data.decode('utf-8')))
    return df 


def get_models(s3,bucket_name, model_key):
    
    obj = s3.get_object(Bucket=bucket_name, Key=model_key)
    model_data = obj['Body'].read()

    # Load LightGBM model
    model = lgb.Booster(model_str=model_data.decode('utf-8'))
    return model  
    
def preprocess_data():
    # TODO: Dan MLmodel.getgee
    # TODO: Dan MLmodel.data_preprocess
    
   


def write_results_to_db(data, mongo_client_str, db_str,  collection_str):
  
    mongo_client = pymongo.MongoClient(mongo_client_str)
    db = mongo_client[ db_str]
    collection = db[collection_str]

    # Insert data into MongoDB
    collection.insert_one(data)
    
    return {
       'statusCode': 200,
        'body': json.dumps('Data written to MongoDB successfully.')
    }


def lambda_handler(event, context):
    # S3 Setup
    s3 = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    try:
        # Get the object from S3
        Bucket=bucket_name
        Key=file_key
        file_content = get_s3_data(s3,Bucket, Key)

        # Process the file content (assuming JSON for this example)
        data = json.loads(file_content.decode('utf-8'))
        
        # MongoDB Setup
        mongo_client_str = "mongodb://username:password@host:port/"
        db_str = 'your_database'
        collection_str = 'your_collection'

        # Insert data into MongoDB
        write_results_to_db(data, mongo_client_str, db_str,  collection_str

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing the file.')
        }




if __name__ == '__main__':

    session = boto3.Session(
    aws_access_key_id='YOUR_ACCESS_KEY',
    aws_secret_access_key='YOUR_SECRET_KEY',
    region_name='YOUR_REGION'
    )
    S3 = session.client('s3')




