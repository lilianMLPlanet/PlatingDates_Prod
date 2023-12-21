
# imports
import os.path
import geopandas as gpd
import pandas as pd
import lightgbm as lgb
import logging
from get_connections import get_s3, mongo_write
from general_methods import read_csv_file, read_csv_files_to_dfs, get_file_names
from cons import *

from baseline_PD import DataPreparation, ModelBaseline
# PD lib
import pw_pdlib
# from pw_pdlib import ml_pd_predict
# from pw_pdlib import ml_pd_prepare



# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_gee_data(action):
    actions = {
        'Model': (get_s3, [TYPE_DATA_MODELS, BUCKET_NAME, PATH_PREFIX_MODELS, '.txt']),
        'COH': (get_s3, [TYPE_DATA_COH, BUCKET_NAME, PATH_PREFIX_DATA_COH, '.csv']),
        'indx': (get_s3, [TYPE_DATA_INDX, BUCKET_NAME, PATH_PREFIX_DATA_INDX, '.csv']),
        'geojson': (get_s3, [TYPE_DATA_GEOJSON, BUCKET_NAME, PATH_PREFIX_DATA_GEOJSON, '.gpkg'])
    }

    if action == 'all':
        for func, args in actions.values():
            logging.info(f"all data is downloading {args}")
            func(*args)
           
    elif action in actions:
        func, args = actions[action]
        logging.info(f"Executing {action}")
        func(*args)
    else:
        logging.error(f"Invalid action specified: {action}")





def lambda_handler(event, context):


    path = os.getcwd()
    path_s2_stats = os.path.join(path, "tmp", TYPE_DATA_INDX)
    path_geopckg = os.path.join(path, "tmp", TYPE_DATA_GEOJSON)
    path_coh = os.path.join(path, "tmp", TYPE_DATA_COH)
    path_models = os.path.join(path, "tmp", TYPE_DATA_MODELS)
    path_preproces_data = os.path.join(path, "tmp", TYPE_DATA_PREPROCES)
    path_out = os.path.join(path, "tmp", RESULTS_MODEL)    
    path_out_final = os.path.join(path, "tmp", FINAL_RESULTS)


    # Example: using S3 event to get file information
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']

    #======================================================================
    # Downloading the data
    #======================================================================
    
    get_gee_data('indx')
    
    #=======================================================================
    # Data processingc
    #=======================================================================
    
    file_names = get_file_names(path_s2_stats, file_extension=".csv")
    
    dataframes = read_csv_files_to_dfs(path_s2_stats,file_names)
    processor = DataPreparation(path_s2_stats, path_coh, path_preproces_data, dataframes['msavi2_median_df'], dataframes['blueMean_mean_df'], dataframes['blueMedian_median_df'], dataframes['bsi_median_df'], dataframes['rvi_median_df'])
    geodata = processor.process_geodata(path_geopckg)
    coh_data = processor.prepare_comb_coh_file("comb_cohMedian_2023.csv")
    processor.prepare_raw_stats(geodata, coh_data, raw_stats_exists=False)

    #=======================================================================
    #  Mongo Db saving results
    #=======================================================================

    
    mongo_write(STR_CONN_DEV, DB_NAME, COLL_NAME, coh_data)



    # Return a response or result
    return {
        'statusCode': 200,
        'body': 'Processing completed successfully'
    }
  


if __name__ == '__main__':
    context_ = []
    event_ = []
    lambda_handler(event_, context_)