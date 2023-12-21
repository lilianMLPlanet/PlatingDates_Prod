import boto3
import json
import pymongo
from io import BytesIO
from general_methods import read_csv_file
from pymongo import MongoClient
import pandas as pd
import os
import logging



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_s3(type_data, bucket_name, path_prefix, end_extension):
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=path_prefix)

        if 'Contents' not in response:
            logging.warning(f"No objects found in the bucket with the specified prefix. {response}")
            return

        for object_summary in response['Contents']:
            key = object_summary['Key']
            local_dir = f'tmp/{type_data}'
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)
            local_file_path = os.path.join(local_dir, key.split('/')[-1])

            #logging.info(f"Key: {key}, Local File Path: {local_file_path}")

            if local_file_path.endswith(end_extension):
                s3.download_file(bucket_name, key, local_file_path)
                logging.info(f"Downloaded: {local_file_path}")

    except Exception as e:
        logging.error(f"An error occurred: {e}")



def mongo_write(client_str, db_name, coll_name, df):
    try:
        # Validate DataFrame
        if df.empty:
            logging.warning("The DataFrame is empty. No data to insert into MongoDB.")
            return

        # Establish MongoDB Connection
        client = pymongo.MongoClient(client_str)
        db = client[db_name]
        collection = db[coll_name]

        # Convert DataFrame to Dictionary
        data_dict = df.to_dict("records")

        # Insert Data into MongoDB
        result = collection.insert_many(data_dict)
        logging.info(f"Inserted {len(result.inserted_ids)} records into {db_name}.{coll_name} successfully.")

    except pymongo.errors.ConnectionFailure as e:
        logging.error(f"Could not connect to MongoDB: {e}")
    except pymongo.errors.OperationFailure as e:
        logging.error(f"Operation failed: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")


def main():
    type_data = 'models'
    bucket_name = 'proag-data'
    path_prefix = 'planting-dates/pd-models-2024/stats-v2-models/'
    end_extension = '.txt'
    get_s3(type_data, bucket_name, path_prefix, end_extension)

    # #======================================================================
    
    # # type_data = 'data/coh_stats'
    # # bucket_name = 'proag-data'
    # # path_prefix = 'planting-dates/pd-data-2023-2024/PA23predictionML_1_3to9_7_rawCOH/'
    # # end_extension = '.csv'
    # get_s3(type_data, bucket_name, path_prefix, end_extension )

    
    #====================================================================== 
   
    #mongodb+srv://cluster1.gmxcy.mongodb.net/?authSource=%24external&authMechanism=MONGODB-AWS
    # data = {'Column1': [], 'Column2': []}
    # df = pd.DataFrame(data)
    # client_str ='mongodb+srv://cluster1.gmxcy.mongodb.net/?authSource=%24external&authMechanism=MONGODB-AWS'
    # db_name = 'algo_results_dev'
    # coll_name = 'planting_dates_2024'
    # mongo_write(client_str, db_name, coll_name, df)





if __name__ == "__main__":
    main()  