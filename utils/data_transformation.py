import os 
import pandas as pd
from logs.application_logging import logger

class DataTransformer:
    def __init__(self):
        self.good_data_path = "data/training_data/good_data_raw"
        self.logger = logger.AppLogger()

    def replace_missing_with_null(self):
        """
        Replaces missing values in CSV files in the 'good_data_raw' directory with 'NULL'.
        Also, removes the first six characters from the 'Wafer' column of each CSV file.
        
        Parameters:
            self (DataTransformer): The DataTransformer instance.
        
        Returns:
            None
        
        Raises:
            Exception: If an error occurs while replacing the missing values with 'NULL'.
        """
        with open('logs/training_logs/data_transformation_logs.txt', 'a+') as log_file:
            try:
                files = [f for f in os.lisdir(self.good_data_path)]
                for file in files:
                    csv_file = pd.read_csv(self.good_data_path + "/" + file)
                    csv_file.fillna("NULL", inplace = True)
                    csv_file['Wafer'] = csv_file['Wafer'].str[6:]
                    csv_file.to_csv(self.good_data_path + "/" + file, index = None, header = True)
                    self.logger.log(log_file, "Missing Values Replaced with NULL for the file: " + file)

            except Exception as e:
                self.logger.log(log_file, "Error occured while replacing the missing values with NULL: " + e)
                raise Exception
