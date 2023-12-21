import os
import shutil
import pandas as pd
import geopandas as gpd
import lightgbm as lgb
import logging
import pw_pdlib.ml_pd_prepare
import pw_pdlib.ml_pd_predict
from general_methods import read_csv_file, read_csv_files_to_dfs, get_file_names
from values_input import *
import sys




class StdoutToLogger:
    """
    Custom class to redirect stdout to logging.
    """
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        # Avoid empty lines and log the message
        if message.rstrip() != "":
            self.logger.log(self.level, message.rstrip())

    def flush(self):
        pass

class DataPreparation:
    def __init__(self, in_dir_raw, in_dir_raw_coh, export_dir, msavi2_df, blueNoCLDF_df, blue_df, bsi_df, rvi_df):
        logging.basicConfig(filename='app.log', level=logging.INFO)
        logger = logging.getLogger('LoggerName')

        # Redirect stdout to the logger
        sys.stdout = StdoutToLogger(logger, logging.INFO)

        self.in_dir_raw = in_dir_raw
        self.in_dir_raw_coh = in_dir_raw_coh
        self.export_dir = export_dir
        self.msavi2_df = msavi2_df
        self.blueNoCLDF_df = blueNoCLDF_df
        self.blue_df = blue_df
        self.bsi_df = bsi_df
        self.rvi_df = rvi_df
       


    def process_geodata(self, file_path, epsg_code=3857):
        """
        Process a GeoPackage file and perform various operations on it.

        :param file_path: Path to the GeoPackage file.
        :param epsg_code: EPSG code for the CRS transformation. Default is 3857.
        :return: Processed GeoDataFrame.
        """
        try:
            file_geo = get_file_names(file_path, file_extension=".gpkg") 
            vec = gpd.read_file(file_geo[0])

            vec = vec.to_crs(epsg=epsg_code)
            vec['centroid'] = vec.geometry.centroid
            vec['commonland'] = vec['CommonLandUnitId']
            vec['x'] = vec.centroid.x
            vec['y'] = vec.centroid.y
            vec.rename(columns={'StateAbbreviation': 'stateabbre', 'clucalcula': 'CluCalcula'}, inplace=True)
            vec['PWId'] = vec['PWId'].astype(str)

            return vec

        except FileNotFoundError:
            logging.error(f"File not found: {file_path}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

    def prepare_comb_coh_file(self, comb_coh_file):
        try:
            if not os.path.exists(comb_coh_file):
                logging.info("Preparing comb_cohMedian_2023.csv file...")
                pw_pdlib.ml_pd_prepare.prepare_raw_gee_stats(date_start_input, date_end_input, True, month_start, self.in_dir_raw, self.in_dir_raw_coh)

            coh_df = pd.read_csv(comb_coh_file)
            
            for item in os.listdir(self.in_dir_raw):
                item_path = os.path.join(self.in_dir_raw, item)
    
                 # Check if the item is a directory
                if os.path.isdir(item_path):
                        # Delete the directory
                    shutil.rmtree(item_path)
                    print(f"Deleted subfolder: {item_path}")
                else:
                    
                    print(f"Skipping file: {item_path}")

            return coh_df

        except FileNotFoundError:
            logging.error(f"File not found: {comb_coh_file}")
        except pd.errors.EmptyDataError:
            logging.error(f"Empty file: {comb_coh_file}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

   
    def prepare_raw_stats(self,vector, coh_df, raw_stats_exists=False):
        """
        Prepares raw statistics if they do not already exist.

        :param raw_stats_exists: Boolean indicating if raw statistics already exist.
        """
        try:
            if not raw_stats_exists:
                logging.info("Preparing raw statistics...")
                pw_pdlib.ml_pd_prepare.prepare_raw_stats(client_names, vector, self.export_dir, self.msavi2_df, self.blueNoCLDF_df, self.blue_df, self.bsi_df, self.rvi_df, coh_df, date_start_input, date_end_input, date_base, month_start)
        except Exception as e:
            logging.error(f"An error occurred while preparing raw statistics: {e}")


# Example usage:

class ModelBaseline:
    def __init__(self, client_names, vec, export_dir, in_dir_raw, in_dir_raw_coh, in_dir_fill, model_path, conf_model_path, task_name):
        self.client_names = client_names
        self.vec = vec
        self.export_dir = export_dir
        self.in_dir_raw = in_dir_raw
        self.in_dir_raw_coh = in_dir_raw_coh
        self.in_dir_fill = in_dir_fill
        self.model_path = model_path
        self.conf_model_path = conf_model_path
        self.task_name = task_name

    # Existing methods ...

    def load_models(self):
        try:
            self.model = lgb.Booster(model_file=self.model_path)
            self.conf_model = lgb.Booster(model_file=self.conf_model_path)
            self.looped_csv_4join = pd.read_csv(os.path.join(self.in_dir_fill, "AR23pd_1_3to9_7.csv"))
        except Exception as e:
            logging.error(f"Error loading models or data: {e}")

    def predict_and_save(self, date_start, date_end, month_start):
        try:
            self.cpds = pw_pdlib.ml_pd_predict.ml_pd_predict.predict(
                self.client_names, self.vec, self.msavi2_df, self.bsi_df, 
                self.rvi_df, self.coh_df, self.looped_csv_4join, 
                self.model, self.conf_model, date_start, date_end, month_start
            )

            out_dir = "output"
            if not os.path.exists(out_dir):
                os.makedirs(out_dir)

            out_file_name = self.task_name + ".csv"
            self.cpds.to_csv(os.path.join(out_dir, out_file_name))

        except Exception as e:
            logging.error(f"Error in prediction or saving results: {e}")

    def apply_coh_fix_and_save(self):
        try:
            # Re-read data for COH fix
            self.msavi2_df = pd.read_csv(os.path.join(self.in_dir_raw, "msavi2_median_df.csv"))
            self.blueNoCLDF_df = pd.read_csv(os.path.join(self.in_dir_raw, "blueMean_mean_df.csv"))
            self.blue_df = pd.read_csv(os.path.join(self.in_dir_raw, "blueMedian_median_df.csv"))
            self.coh_df = pd.read_csv(os.path.join(self.in_dir_raw_coh, "comb_cohMedian_2023.csv"))

            # COH fix
            cpds_final = pw_pdlib.ml_pd_predict.ml_pd_predict.fix_coh_drop_rise(
                self.client_names, self.vec, self.cpds, 
                self.msavi2_df, self.blueNoCLDF_df, self.blue_df, self.coh_df, 
                self.date_start_input, self.date_end_input
            )

            # Save final output after COH fix
            out_fix_file_name = self.task_name + "_cohFixMerge.csv"
            cpds_final.to_csv(os.path.join("output", out_fix_file_name))

        except Exception as e:
            logging.error(f"Error in COH fix or saving final results: {e}")

# Example usage:
# Initialize the DataPreparation class with necessary parameters
            
# processor = GeoDataProcessor("path/to/in_dir_raw", "path/to/in_dir_raw_coh")
# geodata = processor.process_geodata("path/to/geo/package")
# coh_data = processor.prepare_comb_coh_file("path/to/comb_cohMedian_2023.csv", "date_start", "date_end")


# data_preparation = DataPreparation(client_names, vec, export_dir, in_dir_raw, in_dir_raw_coh, in_dir_fill, "model_file_path", "conf_model_file_path", "task_name")

# # Call the methods as needed
# data_preparation.load_models()
# data_preparation.predict_and_save(date_start_input, date_end_input, month_start='03')
# data_preparation.apply_coh_fix_and_save()
