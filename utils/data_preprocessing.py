import numpy as np
import pandas as pd
from sklearn.impute import KNNImputer

from utils import raw_validation, data_transformation, database_operations
from logs.application_logging import logger
# flake8: noqa: E501

class data_validation:
    def __init__(self, data_path):
        """
        This is the constructor for the train_validation class.
        It initializes the class variables and objects.

        Parameters:
            data_path (str): The path to the directory containing the data files.
        """
        self.log = logger.AppLogger()
        self.database_ops = database_operations.DatabaseOperations()
        self.data_transformer = data_transformation.DataTransformer()
        self.data_validator = raw_validation.RawDataValidator(data_path)
        self.log_file = open("logs/training_logs/data_preprocessing_logs.txt", 'a+')

    def raw_data_validation(self):
        """
        This function performs validation and transformation of the raw data.
        It creates a training database and table based on the given schema,
        inserts the data into the table, deletes the Good Data folder, moves
        bad files to the Archive, and extracts a CSV file from the table.
        All the logs are recorded in a log file.

        Returns:
            None

        Raises:
            Exception: If any exception occurs during the execution of the function.
        """
        try:
            self.log.log(self.log_file, "Starting Validation of Files !!!")
            length_of_datestamp, length_of_timestamp, column_name, no_of_columns = self.data_validator.values_from_schema()
            regex = self.data_validator.manual_regex_creation()
            self.data_validator.validate_file_name(regex, length_of_datestamp, length_of_timestamp)
            self.data_validator.validate_column_length(no_of_columns)
            self.data_validator.validate_missing_values()
            self.log.log(self.log_file, "End Validation of Files !!!")

            self.log.log(self.log_file, "Starting Data Transformation !!!")
            self.data_transformer.replace_missing_with_null()
            self.log.log(self.log_file, "End Data Transformation !!!")

            self.log.log(self.log_file, "Creating training_database and tables on the bases of given schema !!!")
            self.database_ops.create_table_db('Training', column_name)
            self.log.log(self.log_file, "Table creation Completed !!!")

            self.log.log(self.log_file, "Insertion of data into table started !!!")
            self.database_ops.insert_into_table('Training')
            self.log.log(self.log_file, "Insertion in table completed !!!")

            self.log.log(self.log_file, "Delete Good Data Folder!!!")
            self.data_validator.delete_existing_good_data_folder()
            self.log.log(self.log_file, "Good_Data folder deleted!!!")

            self.log.log(self.log_file, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            self.data_validator.move_bad_files_to_archive()
            self.log.log(self.log_file, "Bad files moved to archive!!!")

            self.log.log(self.log_file, "Validation Operation Completed!!!")
            self.log.log(self.log_file, "Extracting csv file from table")
            self.database_ops.selecting_data_from_table_into_csv('Training')
            self.log.log(self.log_file, "Csv file extracted successfully!!!")

            self.log_file.close()

        except Exception:
            raise Exception


class data_preprocessing():
    def __init__(self, log_file, logger_object):
        self.logger = logger_object
        self.log_file = log_file

    def remove_columns(self, data, column_name):
        self.logger.log(self.log_file, 'Entered the remove_columns method of the data_preprocessing class')
        self.data = data
        self.column_name = column_name
        try:
            self.useful_data = self.data.drop(labels = self.column_name, axis = 1)
            self.logger.log(self.log_file, 'Column dropped successfully. Exited the remove_columns method of the data_preprocessing class')
            return self.useful_data
        except Exception as e:
            self.logger.log(self.log_file, 'Exception occured in remove_columns method of the data_preprocessing class. Exception message:  ' + str(e))
            self.logger.log(self.log_file, 'Column not dropped. Exited the remove_columns method of the data_preprocessing class')
            raise Exception()

    def seperate_label_feature(self, data, label_column_name):
        self.logger.log(self.log_file, 'Entered the seperate_label_feature method of the data_preprocessing class')
        try:
            self.feature = data.drop(labels = label_column_name, axis = 1)
            self.label = data[label_column_name]
            self.logger.log(self.log_file, 'Label and Feature Separation Successful. Exited the seperate_label_feature method of the data_preprocessing class')
            return self.feature, self.label
        except Exception as e:
            self.logger.log(self.log_file, 'Exception occured in seperate_label_feature method of the data_preprocessing class. Exception message:  ' + str(e))
            self.logger.log(self.log_file, 'Label and Feature Separation Unsuccessful. Exited the seperate_label_feature method of the data_preprocessing class')
            raise Exception()

    def is_null_present(self, data):
        self.logger.log(self.log_file, 'Entered the is_null_present method of the data_preprocessing class')
        self.null_present = False
        try:
            self.null_counts = data.isna().sum()
            for count in self.null_counts:
                if count > 0:
                    self.null_present = True
                    break
            if self.null_present is True:
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                dataframe_with_null.to_csv('data/preprocessing_data/null_values.csv')
            self.logger.log(self.log_file, 'Finding missing values is a success. Exited the is_null_present method of the data_preprocessing class')
            return self.null_present
        except Exception as e:
            self.logger.log(self.log_file, 'Exception occured in is_null_present method of the data_preprocessing class. Exception message:  ' + str(e))
            self.logger.log(self.log_file, 'Finding missing values failed. Exited the is_null_present method of the data_preprocessing class')
            raise Exception()

    def handle_missing_values(self, data):
        self.logger.log(self.log_file, 'Entered the handle_missing_values method of the data_preprocessing class')
        self.data = data
        try:
            impluter = KNNImputer(n_neighbors = 3, weights = 'uniform', missing_values = np.nan)
            self.new_array = impluter.fit_transform(self.data)
            self.logger.log(self.log_file, 'Imputing values Successful. Exited the handle_missing_values method of the data_preprocessing class')
            return self.new_array
        except Exception as e:
            self.logger.log(self.log_file, 'Exception occured in handle_missing_values method of the data_preprocessing class. Exception message:  ' + str(e))
            self.logger_object.log(self.log_file,'Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise Exception()

    def get_columns_with_zero_std_deviation(self, data):
        self.logger.log(self.log_file, 'Entered the get_column_with_zero_std_deviation method of the data_preprocessing class')
        self.columns = self.data.columns
        self.data_describe = self.data.describe()
        self.col_to_drop = []
        try:
            for x in self.columns:
                if (self.data_describe[x]['std'] == 0):
                    self.col_to_drop.append(x)
            self.logger.log(self.log_file, 'Column search for Standard Deviation of Zero Successful. Exited the get_column_with_zero_std_deviation method of the data_preprocessing class')
            return self.col_to_drop
        except Exception as e:
            self.logger.log(self.log_file, 'Exception occured in get_column_with_zero_std_deviation method of the data_preprocessing class. Exception message:  ' + str(e))
            self.logger.log(self.log_file, 'Column search for Standard Deviation of Zero Failed. Exited the get_column_with_zero_std_deviation method of the data_preprocessing class')
            raise Exception()
