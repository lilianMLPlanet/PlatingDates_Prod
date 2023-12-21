
import os
import geopandas as gpd
import pandas as pd
import lightgbm as lgb
import logging


def read_csv_file(file_path):
    try:
        
        df = pd.read_csv(file_path)
        logging.info(f"Successfully read {file_path}")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
    except pd.errors.EmptyDataError:
        logging.error(f"File is empty: {file_path}")
    except Exception as e:
        logging.error(f"Error reading {file_path}: {e}")


def read_csv_files_to_dfs(directory, file_names):
    """
    Reads a list of CSV files into pandas DataFrames.

    :param file_names: List of CSV file names.
    :return: Dictionary of DataFrames keyed by file names (without the '.csv' extension).
    """

    dataframes = {}
    for file_name in file_names:
        df_name = file_name.replace('.csv', '') # Remove .csv extension to create DataFrame name
        path_name = os.path.join(directory, file_name)
        dataframes[df_name] = read_csv_file(path_name)
    return dataframes


def get_file_names(directory, file_extension=".csv"):
    """
    Get a list of file names in the specified directory with the given file extension.

    :param directory: The directory to search for files.
    :param file_extension: The file extension to filter by.
    :return: List of file names with the specified extension.
    """
    try:
        file_names = [f for f in os.listdir(directory) if f.endswith(file_extension)]
        return file_names
    except FileNotFoundError:
        logging.error(f"The directory {directory} was not found.")
    except PermissionError:
        logging.error(f"Permission denied: unable to access the directory {directory}.")
    except Exception as e:
        logging.error(f"An error occurred while accessing {directory}: {e}")




